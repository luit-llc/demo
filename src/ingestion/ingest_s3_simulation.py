import os
from datetime import datetime
import hashlib
from pathlib import Path
from datetime import datetime
import shutil

"""
This class is used as a simulation for S3 into local development
Allowing pipeline to be developed locally with AWS
"""

class IngestionS3Simulator:
	def __init__(self, raw_path="data/raw"):
		# Initialized with the storage path of the file
		self.raw_path = Path(raw_path)
		"""
		mkdir if it doesnot exists
		parents=True - creates parent directory if doesn't exists
		exists_ok=True - doesnt raise error if directory already exists
		"""
		self.raw_path.mkdir(parents=True, exist_ok=True)

	

	def _calculate_md5(self, file_path):
	    """
	    Calculates the MD5 checksum of a file.

	    Args:
	        file_path (str): The path to the file.

	    Returns:
	        str: The MD5 checksum of the file in hexadecimal format, or None if the file is not found.
	    """
	    md5_hash = hashlib.md5()
	    try:
	        with open(file_path, "rb") as f:
	            # Read and update hash in chunks of 4KB
	            for byte_block in iter(lambda: f.read(4096), b""):
	                md5_hash.update(byte_block)
	        return md5_hash.hexdigest()
	    except FileNotFoundError:
	        #print(f"Error: File '{file_path}' not found.")
	        return None



	"""
	Upload file with full metadata tracking

	Returns:
		Metadata dictionary
	"""
	def upload_file(self, client_id: str, file_path: str, content: bytes = None) -> dict:
		#timestamp for file version
		timestamp = datetime.now().strftime('%y%m%d_%H%M%S')
		# extract filename from the full path
		filename = Path(file_path).name
		#create client specific folder
		client_path = self.raw_path / client_id
		client_path.mkdir(exist_ok=True)
		#create unique target filename
		target_path = client_path / f"{timestamp}_{filename}"

		#write the file content
		if content:
			with open(target_path, 'wb') as f:
				f.write(content)
		else:
			shutil.copy2(file_path, target_path)

		file_hash = self._calculate_md5(target_path)
		return {
			"client_id": client_id,
			"file_name": file_path,
			"ingestion_time": datetime.now(),
			"file_size": target_path.stat().st_size,
			"checksum": file_hash,
			"local_path": str(target_path)
		}

	def delete_file(self, client_id: str, filename: str) -> bool:

		file_path = self.raw_path / client_id / filename
		if file_path.exists():
			file_path.unlink()
			return True
		return False

	def get_metadata(self, client_id: str, filename: str) -> dict:

		file_path = self.raw_path / client_id / filename
		if not file_path.exists():
			return None

		return {
			"client_id": client_id,
			"file_name": filename,
			"file_size": file_path.stat().st_size,
			"checksum": self._calculate_md5(file_path)
		}
