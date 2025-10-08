"""
Test for ingest_s3_simulation.py
"""
import os
import sys
import json
from pathlib import Path
from src.ingestion.ingest_s3_simulation import IngestionS3Simulator


def test_ingest_s3_simulation():
	print("TEsting IngestionS3Simulator")

	simulator = IngestionS3Simulator(raw_path="tests/test_raw_data")

	test_file = Path("test_member_data.csv")
	test_file.write_text("member_id, first_name, last_name\n123, John, Doe\n")

	#Test the upload method
	metadata = simulator.upload_file("test_client", str(test_file))

	print("Upload success")

	simulator.delete_file("test_client", "251001_180439_test_member_data.csv")
	
if __name__ == "__main__":
	test_ingest_s3_simulation()