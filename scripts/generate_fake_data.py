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
        logging.FileHandler('logs/generate_fake_data.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


# Configure Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# API data generation
# Define the date range for the data, to keep data relevant to the current year
start_datetime = dt(2022, 1, 1, 10, 30)
end_datetime = dt(2024, 12, 31, 23, 59)


def generate_data(num_rows=8000):
    """Generate and save fake data to JSON, Parquet, and CSV files.

    Args:
        num_rows (int): Number of records to generate. Defaults to 8000.

    Returns:
        list: List of dictionaries containing fake data records.

    """

    data = []
    for _ in range(num_rows):
        login_time = fake.date_time_this_year()
        logout_time = login_time + timedelta(hours=random.uniform(0.5, 4))
        session_duration = (logout_time - login_time).total_seconds() / 60
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        record = {
            "user_id": fake.uuid4(),
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name}_{last_name}@example.com",
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
            "login_time": login_time.isoformat(),
            "logout_time": logout_time.isoformat(),
            "session_duration_minutes": round(session_duration, 2),
            "product_id": fake.uuid4(),
            "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega"]),
            "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
            "purchase_status": fake.random_element(["completed", "pending", "failed"]),
            "device_type": random.choice(["Desktop", "Tablet", "Mobile"]),
            "os": random.choice(["Windows", "macOS", "iOS", "Android", "Linux"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"]),
        }
        data.append(record)
    return data


# Error check for file generation
def save_data():
    """Generate and save data to JSON, Parquet, and CSV files for different use cases.
    """
    try:
        fake_data = generate_data()
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Save data to JSON and Parquet formats for different use cases
        json_file = os.path.join(
            script_dir, "..", "data", "simulated_api_data.json")
        parquet_file = os.path.join(
            script_dir, "..", "data", "simulated_api_data.parquet")
        csv_file = os.path.join(script_dir, "..", "data",
                                "simulated_api_data.csv")

        # Write JSON
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(fake_data, f, indent=4)

        # Write Parquet
        pl.DataFrame(fake_data).write_parquet(parquet_file)

        # Write CSV
        pl.DataFrame(fake_data).write_csv(csv_file)

        log.info("Data successfully generated and saved to %s.", script_dir)

    except (IOError, OSError) as e:
        log.error("Error writing to file %s: %s", script_dir, e)
        sys.exit(1)
    except ValueError as e:
        log.error("Unexpected error generating/saving data: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    save_data()
