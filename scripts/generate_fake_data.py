import json
import logging
import os
import random
import sys
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import polars as pl
import yaml
from faker import Faker

log_dir = Path('logs')
log_dir.mkdir(parents=True, exist_ok=True)

log_file_path = log_dir / 'generate_fake_data.log'

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

num_rows = 8000
start_datetime = dt(2022, 1, 1, 10, 30)
end_datetime = dt(2024, 12, 31, 23, 59)

# Configure Faker
fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_data():
    """Generate and save fake data to JSON, Parquet, and CSV files."""
    try:
        data = []
        # Add data validation during generation
        for _ in range(num_rows):
            # Persist user data for each record
            first_name = fake.first_name()
            last_name = fake.last_name()

            # Generates data around account creation, deletion and updates
            is_active = 1 if random.random() < 0.8 else 0
            account_created = fake.date_time_between(
                start_date=start_datetime, end_date=end_datetime)
            account_updated = fake.date_time_between(
                start_date=account_created, end_date=end_datetime)
            account_deleted = fake.date_time_between(
                start_date=account_updated, end_date=end_datetime) if is_active == 0 else None

            # Generates login and logout times for each record
            login_time = fake.date_time_between(
                start_date=account_created,
                end_date=account_deleted if account_deleted else end_datetime
            )

            # This guarantees that logout time is always .5 to 4 hours after login time
            logout_time = login_time + timedelta(hours=random.uniform(0.5, 4))

            # Calculates session duration in minutes
            session_duration = (logout_time - login_time).total_seconds() / 60

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
                "is_active": is_active,
                "login_time": login_time.isoformat(),
                "logout_time": logout_time.isoformat(),
                "account_created": account_created.isoformat(),
                "account_updated": account_updated.isoformat(),
                "account_deleted": account_deleted.isoformat() if account_deleted else None,
                "session_duration_minutes": round(session_duration, 2),
                "product_id": fake.uuid4(),
                "product_name": fake.random_element(["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi", "Rho", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega"]),
                "price": round(fake.pyfloat(min_value=100, max_value=5000, right_digits=2), 2),
                "purchase_status": fake.random_element(["completed", "pending", "failed"]),
                "user_agent": fake.user_agent()
            }

            # Validate record before adding
            if not all([
                record['login_time'] < record['logout_time'],
                record['account_created'] <= record['account_updated'],
                record['price'] > 0
            ]):
                continue

            data.append(record)

        # Add basic statistics logging
        log.info("Generated %s records", num_rows)
        log.info("Active users: %s", sum(1 for r in data if r['is_active']))
        log.info("Average price: $%s", sum(r['price'] for r in data)/len(data))

        return data

    except Exception as e:
        log.error("Error generating data: %s", str(e))
        raise


# Error check for file generation
def save_data():
    """Generate and save data to JSON, Parquet, and CSV files for different use cases.
    """
    try:
        fake_data = generate_data()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(os.path.dirname(script_dir), "data")

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

        log.info("Data successfully generated and saved to %s.", data_dir)

    except (IOError, OSError) as e:
        log.error("Error writing to file %s: %s", data_dir, e)
        sys.exit(1)
    except ValueError as e:
        log.error("Unexpected error generating/saving data: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    save_data()
