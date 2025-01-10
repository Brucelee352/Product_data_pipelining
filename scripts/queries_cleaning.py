import duckdb
import pandas as pd

# Load the CSV file into a DataFrame for cleanup
df = pd.read_csv('C:/Users/bruce/Documents/GitHub/Product_data_pipelining/data/simulated_api_data.csv')

# Data cleaning:
## Apply strip only to string columns
for col in df.select_dtypes(include=['object']).columns:
    df[col] = df[col].str.strip()
    
# Remove duplicate email addresses
df = df.drop_duplicates(subset=['email'])

# Remove columns that are not relevant to the analysis
for col in df:
    if col in ['phone_number', 'email_address', 'city', 'postal_code', 'country', 'product_id']:
        df = df.drop(col, axis=1)
        
# add a new column for country
df['country'] = 'United States'
df = df[['user_id', 'first_name', 'last_name', 'email', 
         'date_of_birth', 'address', 'state', 'country', 
         'company', 'job_title', 'ip_address', 'login_time', 
         'logout_time', 'session_duration_minutes', 'product_name', 
         'price', 'purchase_status', 'device_type', 'os', 'browser']]

# Save cleaned data to CSV
df.to_csv('C:/Users/bruce/Documents/GitHub/Product_data_pipelining/data/cleaned_data.csv', index=False) # Save cleaned data to CSV

conn = duckdb.connect(':memory:')

# Load the cleaned CSV into DuckDB
conn.execute("""
    CREATE TABLE user_activity AS
    SELECT * FROM read_csv_auto('C:/Users/bruce/Documents/GitHub/Product_data_pipelining/data/cleaned_data.csv');
""")

# Example query to calculate session duration and filter invalid timestamps
result = conn.execute("""
    SELECT concat(first_name, ' ', last_name) AS full_name, email, state, login_time, logout_time,
           (logout_time - login_time) AS session_duration,
           EXTRACT(EPOCH FROM (logout_time - login_time)) / 60 AS session_duration_minutes,
           ROUND(EXTRACT(EPOCH FROM (logout_time - login_time)) / 3600, 1) AS session_duration_hours
    FROM user_activity
    WHERE login_time <= logout_time
    ORDER BY state
    LIMIT 50;
    """).fetchdf()

# Display the results
print(result)