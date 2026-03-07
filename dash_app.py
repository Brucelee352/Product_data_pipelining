import os
from portfolio_app.scripts.constants import DBT_PROFILES_DIR
from portfolio_app.app import main

# Set environment variables before anything else
os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_DIR)

if __name__ == "__main__":
    main()
