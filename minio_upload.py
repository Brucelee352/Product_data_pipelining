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
logger = logging.getLogger(__name__)

try:
    logger.info("Attempting to connect to MinIO...")
    client = minio.Minio("localhost:9000", 
                   access_key="admin", 
                   secret_key="password123",
                   secure=False) 
    logger.info("Connected to MinIO")
except Exception as e:
    logger.error(f"Error connecting to MinIO: {e}")
    exit(1)

api_data = Path("C:/Users/bruce/Documents/simulated_api_data.json")

bucket = "sim-api-data"
destination_name = "simulated_api_data.json"

found = client.bucket_exists(bucket)

if not found:
    try:    
        client.make_bucket(bucket)
        logger.info(f"Bucket {bucket} created")
    except Exception as e:
        logger.error(f"Error creating bucket {bucket}: {e}")
else:
    logger.info(f"Bucket {bucket} already exists")

try:
    result = client.fput_object(bucket, destination_name, str(api_data))
    logger.info(f"File {api_data} uploaded to {bucket} as {destination_name}")
except Exception as e:
    logger.error(f"Error uploading file {api_data} to {bucket} as {destination_name}: {e}")


