import os
import json
import random
import logging
import sys
from datetime import timedelta, datetime as dt
from faker import Faker
import polars as pl


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('generate_fake_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


# Configure Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# API data generation
start_datetime = dt(2022, 1, 1, 10, 30)
end_datetime = dt(2024, 12, 31, 23, 59)

def generate_fake_data(num_rows=8000):
    data = []
    for _ in range(num_rows):
        record = {
            "user_id": fake.uuid4(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=72).isoformat(),
            "phone_number": fake.phone_number(),
            "address": fake.address(),
            "city": fake.city(),
            "state": fake.state(),
            "postal_code": fake.postcode(),
            "country": fake.country(),
            "company": fake.company(),
            "job_title": fake.job(),
            "ip_address": fake.ipv4(),
            "login_time": fake.date_time_this_year().isoformat(),
            "logout_time": (fake.date_time_this_year() + timedelta(hours=random.uniform(0.5, 4))).isoformat(),
            "session_duration_minutes": round(random.uniform(30, 240), 2),
            "timestamp": fake.date_time_between(start_date=start_datetime, end_date=end_datetime).isoformat(),
            "product_id": fake.uuid4(),
            "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega"]),
            "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
            "purchase_status": fake.random_element(["completed", "pending", "failed"]),
            "device_type": random.choice(["Desktop", "Tablet", "Mobile"]),
            "os": random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"]),
            "user_agent": fake.user_agent()
        }
        data.append(record)
    return data

# Error check for file generation
try:
    fake_data = generate_fake_data()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    #Save data to JSON and Parquet formats for different use cases
    json_file = os.path.join(script_dir, "..", "data", "simulated_api_data.json")
    parquet_file = os.path.join(script_dir, "..", "data", "simulated_api_data.parquet")
    
    #Write JSON
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(fake_data, f, indent=4)
    
    #Write Parquet
    pl.DataFrame(fake_data).write_parquet(parquet_file)
    log.info("Data successfully generated and saved to %s.", script_dir)

except IOError as e:
    log.error("Error writing to file %s: %s", script_dir, e)
    exit(1)
except Exception as e:
    log.error("Unexpected error generating/saving data: %s", e)
    exit(1)
