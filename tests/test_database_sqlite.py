import sys
import os
import sqlite3
from pathlib import Path

from src.storage.database_sqlite import DataSQLiteStorage

def test_database_initialization():
	test_db_path = "test_radiantgraphdemo_database.db"
	storage = DataSQLiteStorage(db_path=test_db_path)

	if Path(test_db_path).exists():
		print("Database file created success")
	else:
		print("Database file not created")
		return False

test_database_initialization()