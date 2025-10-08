import sys
from pathlib import Path
import pandas as pd



"""
4. Serving & Analytics
● Create a local table (SQLite/Parquet) with normalized data.
● Provide example queries:
○ Count of unique members per client.
○ Top ZIP codes by member count.
○ Ingestion error rate (bad rows / total).
"""
#Adding project root to sys.path
sys.path.append(str(Path(__file__).parent))

from src.storage.database_sqlite import DataSQLiteStorage

def analytics_queries():
	

	
	print("*********************************************\n")
	print("Count of unique members per client\n")
	unique_member_query = """
		SELECT client_id, COUNT(distinct member_id) as unique_member_count
		FROM members
		GROUP BY client_id
	"""

	storage = DataSQLiteStorage()
	conn = storage._db_connection()

	unique_member_df = pd.read_sql(unique_member_query, conn)

	print(unique_member_df)
	print("*********************************************\n")
	print("Top ZIP codes by member count\n")
	top_zip5_query = """
		SELECT zip5, count(*) as member_count
		FROM members
		GROUP BY zip5
		ORDER BY member_count DESC
	"""
	top_zip5_member_df = pd.read_sql(top_zip5_query, conn)
	print(top_zip5_member_df)
	print("*********************************************\n")
	print("Ingestion error rate (bad rows / total)\n")
	ingestion_error_rate_query = "SELECT client_id, sum(invalid_rows) * 1.0 / SUM(total_rows) as ingestion_error_rate from audit_log GROUP BY client_id;"
	ingestion_error_rate_df = pd.read_sql(ingestion_error_rate_query, conn)
	print(ingestion_error_rate_df)

	conn.close()
if __name__ == "__main__":

	analytics_queries()