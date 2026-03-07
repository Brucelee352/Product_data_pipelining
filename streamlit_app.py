import os
from portfolio_app.scripts.constants import DBT_PROFILES_DIR
from portfolio_app.app import main

# Set environment variables before anything else
os.environ['DBT_PROFILES_DIR'] = str(DBT_PROFILES_DIR)

# Note: This app now uses Dash instead of Streamlit.
# Run with: python streamlit_app.py  (or use dash_app.py)
if __name__ == "__main__":
    main()
