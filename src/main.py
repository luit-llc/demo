"""
src
- __init__.py
- main.py
- raw
- audit
- ingestion
- validation
requirements.txt
pipeline.py
"""
import csv
import sys
import os
from pathlib import Path
from typing import Tuple, List, Dict, Any
import pandas as pd
from pydantic import ValidationError
sys.path.append(str(Path(__file__).parent))  #adds the directory containing the current Python script to the list of paths Python searches when importing modules

#importing the custom modules
from audit.logger import AuditLogger	#creates audit for HIPPA compliance
from ingestion.ingest_s3_simulation import IngestionS3Simulator #AWS S3 simulation
from validation.schema_validation import RawMemberSchema #Data validation and schema normalization
from storage.database_sqlite import DataSQLiteStorage #SQLite DB connection and operation
from datetime import datetime

class RadiantGrapgDemoDataPipeline:
	"""
	1.	Client Files		→ 	S3/SFTP
  	↓
	2.	Secure Ingestion 	→ 	AWS S3 + IAM
 	↓
	3.	Validation Engine	→ 	Schema Validation + Transformation
  	↓
	4.	Audit & Compliance	→ 	JSON Audit Logs + Database Logs
  	↓
	5.	Analytics Serving	→ 	SQLite/Parquet + Example Queries
	"""

	def __init__(self):

		#Initialize with all the components
		self.logger = AuditLogger()
		self.ingest_s3_simulation = IngestionS3Simulator()
		#self.schema_validation = RawMemberSchema()
		self.database_sqlite = DataSQLiteStorage()

		#print("Pipeline initialized...Done")

	def data_process(self, client_id: str, file_path: str) -> Dict[str, Any]:
		#2.	Secure Ingestion 	→ 	AWS S3 + IAM
		

		# Step 1 -> Ingestion of the client file
		#upload_file(self, client_id: str, file_path: str, content: bytes = None) -> dict:
		ingestion_metadata = self.ingest_s3_simulation.upload_file(client_id, file_path)
		#print(f"File Ingestion {client_id} {file_path}...Done")

		#Step 2 -> Data Validation
		#print("Step 2 -> Data Validation--------------------")

		
		valid_records, rejected_records = self._process_input_file(
				client_id, 
				ingestion_metadata['local_path'],
				ingestion_metadata['file_name']
			)
		#print(f"Valid Records - {valid_records}")
		#print(f"rejected_records - {rejected_records}")
		#Step 3 -> Store validated data
		self.database_sqlite.insert_members(client_id, valid_records)
		#print(f"Inserted {len(valid_records)} valid records into Table MEMBERS")

		#Step 4 -> Audit and Compliance
		#audit_log(self, valid_row_count: int, rejected_row_count: int, metadata: Dict[str, Any]) -> str:
		audit_log = self.logger.audit_log(len(valid_records), len(rejected_records), ingestion_metadata)

		#Insert into DB as well
		ingestion_time = ingestion_metadata["ingestion_time"]
		if isinstance(ingestion_time, datetime):
			ingestion_time_str= ingestion_time.isoformat()
		else:
			ingestion_time_str=str(ingestion_time)

		file_name_str = str(ingestion_metadata['file_name'])

		self.database_sqlite.insert_audit_log(client_id, file_name_str , len(valid_records), len(rejected_records),  ingestion_time_str)

	def _process_input_file(self, client_id: str, file_path: str, file_name: str) -> Tuple[List[Dict], List[Dict]]:
		valid_records = []
		rejected_records = []

		try:
			df = pd.read_csv(file_path)

			for row in df.to_dict(orient="records"):
				raw_record = RawMemberSchema(raw_data=row)
				normalize_record = raw_record.normalize()

				if normalize_record is None:
					rejected_records.append({ "client_id": client_id,"file_name": file_name,"raw_data": row,"errors": raw_record.errors})
					continue  # skip to next row

				record_dict = normalize_record.dict()
				record_dict['client_id'] = client_id
				valid_records.append(record_dict)

			if rejected_records:
				rejected_path = Path(f"data/rejected/{client_id}")
				rejected_path.mkdir(parents=True, exist_ok=True)
				timestamp = datetime.now().strftime('%y%m%d_%H%M%S')
				rejected_file = rejected_path / f"rejected_{timestamp}.csv"

				with open(rejected_file, 'w', newline="", encoding="utf-8") as f:
					writer = csv.writer(f)
					writer.writerow(["row_data", "error_message"])
					for record in rejected_records:
						row_data = str(record.get("raw_data", {}))
						errors = record.get("errors", [])
						if callable(errors):
							error_message = str(errors())
						else:
							error_message = str(errors)
						#error_message = "; ".join(errors) if isinstance(errors, list) else str(errors)
						writer.writerow([row_data, error_message])


		except Exception as e:
			print(f"Error processing client input file-------------{e}")

		return valid_records, rejected_records


	def _deleteme_process_input_file(self, client_id: str, file_path: str, file_name:str) -> Tuple[List[Dict], List[Dict]]:
		valid_records = []
		rejected_records = []

		"""
		Reads the CSV file.

		Iterates over each row.

		Validates + normalizes the row against schema in MemberSchema).

		If valid --add to valid_records.

		If invalid--add to rejected_records with error details.

		"""
		try:
			#print(f"file_path------------{file_path}")
			df = pd.read_csv(file_path)
			#print(df)
#			for _, row in df.iterrows():  #This works too, but slightly slower on large data because each row is a Pandas Series, not a dict.
#				try:
#					raw_record = RawMemberSchema(row.to_dict())
#					normalize_record = raw_record.normalize()
#					normalize_record['client_id'] = client_id   #adding client_id during normalization:
#					valid_records.append(normalize_record.dict())
#				except ValidationError as e:
#					rejected_records.append({
#						"client_id": client_id,
#						"file_name": file_name,
#						"raw_data": row,
#						"errors": e.errors
#						})

			for row in df.to_dict(orient="records"):
				try:
					raw_record = RawMemberSchema(raw_data=row)
					normalize_record = raw_record.normalize()
					record_dict = normalize_record.dict()
					record_dict['client_id'] = client_id
					valid_records.append(record_dict)
				except ValidationError as e:
					rejected_records.append(
						{
							"client_id": client_id,
							"file_name": file_name,
							"raw_data": row,
							"errors": e.errors
						}
						)

			if rejected_records:
				#print(rejected_records)
				rejected_path = Path(f"data/rejected/{client_id}")
				rejected_path.mkdir(parents=True, exist_ok=True)
				timestamp = datetime.now().strftime('%y%m%d_%H%M%S')
				rejected_file = rejected_path / f"rejected_{timestamp}.csv"

				try:
					with open(rejected_file, 'w', newline="", encoding="utf-8") as f:
						writer = csv.writer(f)
						writer.writerow(["row_data", "error_message"])
						for record in rejected_records:
							row_data = str(record.get("raw_data", {}))
							errors = record.get("errors")
							if callable(errors):
								error_message = str(errors())
							else:
								error_message = str(errors)

							writer.writerow([row_data, error_message])
				except Exception as e:
					print(f"Failed to write rejected rows ---- {e}")
						
		except Exception as e:
			print(f"Error processing client input file-------------{e}")

		return valid_records, rejected_records

def run_pipeline(client_id: str, file_path:str):
	pipeline = RadiantGrapgDemoDataPipeline()
	result = pipeline.data_process(client_id, file_path)

	return result

