"""
Orchestrates the workflow

S3 Simulation - data/raw
create client direcotry - data/raw/clent_001 .... etc
copies client sample data file from sample_inputs to data/rwa/client_001 etc

Audit log created
Ingestion log created




This is a POC simulation program
In real scenerio this will be triggered when client files are dropped in S3 bucket

"""

from pathlib import Path
from src.main import run_pipeline
import shutil

client_id = "client_001"
source_path = Path("sample_inputs")
source_file = "sample_data.csv"

client_file = source_path / source_file
result = run_pipeline(client_id, client_file)
