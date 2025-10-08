This is a demo data pipeline project. It simulates client data ingestion, validation, and storage using Python, Pandas, Pydantic, and SQLite.
Features 
  Secure Ingestion -> Simulated S3 with checksums and metadata
  Data Validation -> Schema validation with error handling
  Analytics -> SQLite3 storage with query example
  Count of unique members per client 
  Top ZIP codes by member count 
  Ingestion error rate (bad rows / total)

Limitations
  S3 Simulation
  Scalability - Designed for demo not production 
  Monitoring -> Basic logging
  Error Handling -> SImple logic



demo/
├── data/
│ ├── audit
│ ├── raw
│ ├── rejected
├── sample_inputs/
├── src/ 
│ ├── init.py
│ ├── main.py # Main pipeline implementation
│ ├── validation/ # Schema validation
│ │ ├── __init__.py
│ │ ├── schema_validation.py
│ ├── ingestion/ # S3 simulation
│ │ ├── __init__.py
│ │ ├── ingest_s3_simulation.py
│ ├── audit/ # Audit logging
│ │ ├── __init__.py
│ │ ├── logger.py
│ ├── storage/ # SQLite DB operations
│ │ ├── __init__.py
│ │ ├── database_sqlite.py
├── tests/ # Unit tests
├── venv/ # Python virtual environment
├── pipeline.py # Entry point for running the pipeline
├── setup.py # Optional setup file
└── README.md


Setup and running the demo project
Clone the repository
  git clone https://github.com/luit-llc/demo.git
  cd demo
Create Python virtual env
  python -m venv venv
  # Windows (I have used windows to create and run this project)
  venv\Scripts\activate
Install dependencies
  pip install -r requirements.txt
  pip install pydantic[email]    =========> For some reson I had to run this seperatly and didn't get much time to debug, for now run this command
Run pipeline
  python pipeline.py
Run Analytics code
  python analytics_queries.py
Run Test
  pytest tests   ==========> Didn't get time to go over all test case senerios

Output:
Valid records inserted in SQLite DB
Rejected records stored in data/rejected/<client_id>/
Audit logs in data/audit/ and DB
  
