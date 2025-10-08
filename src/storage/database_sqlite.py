"""
Data storage using SQLITE 

○ Count of unique members per client.
○ Top ZIP codes by member count.
○ Ingestion error rate (bad rows / total).
"""

import sqlite3
import pandas
from pathlib import Path
from typing import List
from datetime import datetime

class DataSQLiteStorage:
	"""
	Database initialization
	Table creation
	"""

	def __init__(self, db_path = "data/radiantgraphdemo.db"):
		self.db_path = Path(db_path)


		
		self._init_sqlite_database()

	def _init_sqlite_database(self):
		"""
		mkdir if it doesnot exists
		parents=True - creates parent directory if doesn't exists
		exists_ok=True - doesnt raise error if directory already exists
		"""
		Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
		conn = sqlite3.connect(self.db_path)

		#conn = self._db_connection()
		try:

			# MEMBER table
			conn.execute('''
				CREATE TABLE IF NOT EXISTS members (
					member_id INT NOT NULL,
					first_name TEXT NOT NULL,
					last_name TEXT NOT NULL,
					dob TEXT NOT NULL,
					gender TEXT NOT NULL,
					phone TEXT NOT NULL,
					email TEXT,
					zip5 TEXT NOT NULL,
					plan_id TEXT NOT NULL,
					client_id TEXT NOT NULL,
					ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					PRIMARY KEY (member_id, client_id)
				)
				'''
				)

			#AUDIT table
			conn.execute('''
				CREATE TABLE IF NOT EXISTS audit_log (
					audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
					client_id TEXT NOT NULL,
					file_name TEXT NOT NULL,
					total_rows INTEGER NOT NULL,
					valid_rows INTEGER NOT NULL,
					invalid_rows INTEGER NOT NULL,
					ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				)

				'''
				)
			conn.commit()
			#print("table created")
		except sqlite3.Error as e:
			#print("DB initiliazation failed")
			conn.rollback()
		finally:
			conn.close()

	def insert_members(self, client_id: str, members: List[dict]):
		conn = sqlite3.connect(self.db_path)
		try:
			for member in members:
				conn.execute('''
							INSERT INTO members
							(member_id, first_name, last_name, dob, gender, phone, email, zip5, plan_id, client_id)
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
							ON CONFLICT(member_id, client_id) DO UPDATE SET
								first_name=excluded.first_name,
								last_name=excluded.last_name,
								dob=excluded.dob,
								gender=excluded.gender,
								phone=excluded.phone,
								email=excluded.email,
								zip5=excluded.zip5,
								plan_id=excluded.plan_id,
								ingestion_time=CURRENT_TIMESTAMP
						''', (
									member['member_id'],
									member['first_name'],
									member['last_name'],
									member['dob'],
									member['gender'],
									member['phone'],
									member['email'],
									member['zip5'],
									member['plan_id'],
									member['client_id']
							)
					)
				conn.commit()
		finally:
			conn.close()

	def insert_audit_log(self, client_id, file_name, valid_record_count, invalid_record_count, ingestion_time):
		conn = sqlite3.connect(self.db_path)

		

		try:
			conn.execute('''
				INSERT INTO audit_log
				(client_id, file_name, total_rows, valid_rows, invalid_rows, ingestion_time)
				VALUES(?, ?, ?, ?, ?, ?)
				''', (
					client_id,
					file_name,
					valid_record_count+invalid_record_count,
					valid_record_count,
					invalid_record_count,
					ingestion_time
					))
			conn.commit()
		finally:
			conn.close()

	def _db_connection(self) -> sqlite3.Connection:
		"""
		Creates a SQLite db connection
		Returns - connection object
		"""
		conn = sqlite3.connect(self.db_path)
		#conn.execute("")
		return conn
