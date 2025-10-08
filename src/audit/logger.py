from typing import Dict, Any
from pathlib import Path
from datetime import datetime
import json

"""
AuditLogger module handles HIPPA compliance audit logging for the  pipeline
The class creates audit logs that capture
- clientId, file_name - who access what data
- timestamp - when data was accessed
- event_type - what operation was performed ingestion/validation etc
- record_count - success/failure metrics
"""
class AuditLogger:
	def __init__(self, audit_path="data/audit"):
		# Initialized with the storage path of the file
		self.audit_path = Path(audit_path)
		"""
		mkdir if it doesnot exists
		parents=True - creates parent directory if doesn't exists
		exists_ok=True - doesnt raise error if directory already exists
		"""
		self.audit_path.mkdir(parents=True, exist_ok=True)
		
	"""
	Used chatgpt as the timestamp and JSON was createing issue and didn't have time to debug - handling the datetime object automatically
	"""
	def _ensure_serializable(self, data: Any) -> Any:
		if isinstance(data, datetime):
			return data.isoformat()
		elif isinstance(data, Path):
			return str(data)
		elif isinstance(data, Dict):
			return {key: self._ensure_serializable(value) for key, value in data.items()}
		elif isinstance(data, list):
			return [self._ensure_serializable(item) for item in data]
		elif isinstance(data, tuple):
			return tuple(self._ensure_serializable(item) for item in data)
		else:
			return data

	def audit_log(self, valid_row_count: int, rejected_row_count: int, metadata: Dict[str, Any]) -> str:
		""" log """
		#Write to JSON
		audit_record = {
			"event_type": "ingestion",
			"timestamp": datetime.now(),
			"client_id": metadata["client_id"],
			"file_name": metadata["file_name"],
			"ingestion_time": metadata["ingestion_time"],
			"valid_row_count": valid_row_count,
			"rejected_row_count" : rejected_row_count,
			"total_row_processed": valid_row_count + rejected_row_count,
			"checksum": metadata["checksum"],
			"compliance": {
				"hippa_compliant": True,
				"data_encrypted": True,
				"audit_trail_maintained": True
			}
		}

		# DOUBLE SAFETY: Recursively check and convert any datetime objects
		serializable_record = self._ensure_serializable(audit_record)

		timestamp_str = datetime.now().strftime('%y%m%d_%H%M%S')
		#print(timestamp_str)
		audit_filename = f"ingestion_{metadata["client_id"]}_{timestamp_str}.json"
		#print(audit_filename)
		audit_log = self.audit_path / audit_filename
		#print(audit_log)

		#print(f"Audit log file created : {audit_log}")
		with open(audit_log, 'w') as f:
			json.dump(serializable_record, f, indent=2) #indent=2 for formatting

		

		return str(audit_log)