import os
import minio 
from pathlib import Path
import logging
import sys

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('minio_upload.log'),  # Logs to file
        logging.StreamHandler(sys.stdout)         # Logs to console
    ]
)
log = logging.getLogger(__name__)

try:
    log.info("Attempting to connect to MinIO...")
    client = minio.Minio("localhost:9000", 
                   access_key="admin", 
                   secret_key="password123",
                   secure=False) 
    log.info("Connected to MinIO")
except Exception as e:
    log.error("Error connecting to MinIO: %s", str(e))
    exit(1)
    
script_dir = os.path.dirname(os.path.abspath(__file__))    

api_data_json =  os.path.join(script_dir, "..", "data", "simulated_api_data.json")
api_data_parquet = os.path.join(script_dir, "..", "data", "simulated_api_data.parquet")

bucket = "sim-api-data"
destination_name_a = "simulated_api_data.json"
destination_name_b = "simulated_api_data.parquet"

found = client.bucket_exists(bucket)

if not found:
    try:
        client.make_bucket(bucket)
        log.info("Bucket %s created", bucket)
    except Exception as e:
        log.error("Error creating bucket %s: %s", bucket, e)
else:
    log.info("Bucket %s already exists", bucket)

for source, dest in [(api_data_json, destination_name_a), 
                     (api_data_parquet, destination_name_b)]:
    try:
        client.fput_object(bucket, dest, source)
        log.info("File %s uploaded to %s as %s", source, bucket, dest)
    except Exception as e:
        log.error("Error uploading file %s to %s: %s", source, bucket, str(e))
        exit(1)
