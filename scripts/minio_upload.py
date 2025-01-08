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

api_data = Path("C:/Users/bruce/Documents/simulated_api_data.json")

bucket = "sim-api-data"
destination_name = "simulated_api_data.json"

found = client.bucket_exists(bucket)

if not found:
    try: 
        client.make_bucket(bucket)
        log.info("Bucket %s created", bucket)
    except Exception as e:
        log.error("Error creating bucket %s: %s", bucket, e)
else:
    log.info("Bucket %s already exists", bucket)

try:
    result = client.fput_object(bucket, destination_name, str(api_data))
    log.info("File %s uploaded to %s as %s", api_data, bucket, destination_name)
except Exception as e:
    log.error("Error uploading file %s to %s as %s: %s", api_data, bucket, destination_name, e)
    exit(1)
