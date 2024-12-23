from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

PUBLIC_IP = "18.132.73.146"
USERNAME = "consultants"
PASSWORD = "WelcomeItc@2022"
DB_NAME = "testdb"
PORT = "5432"
ENCODED_PASSWORD = quote_plus(PASSWORD)

try:
    # Create database connection
    # database_url = f"postgresql://{USERNAME}:{ENCODED_PASSWORD}@{PUBLIC_IP}:{PORT}/{DB_NAME}"
    # engine = create_engine(database_url, echo=False)
    engine = create_engine('postgresql://consultants:WelcomeItc%402022@18.132.73.146:5432/testdb')
    print("Database connection established.")

    # File path for CSV
    file_path = "C:\\Users\\sujay\\Downloads\\fraudTest.csv\\fraudTest.csv"

    # Read CSV with parsing dates
    result = pd.read_csv(file_path, parse_dates=['trans_date_trans_time'])
    print("CSV file loaded successfully.")

    # Ensure the data is sorted by date
    result = result.sort_values(by='trans_date_trans_time')
    print("Data sorted by transaction date.")

    # Calculate the maximum date within the first 100 days
    start_date = result['trans_date_trans_time'].min()
    end_date = start_date + pd.Timedelta(days=100)

    # Filter the data for the first 100 days
    filtered_data = result[(result['trans_date_trans_time'] >= start_date) & (result['trans_date_trans_time'] < end_date)]
    print(f"Filtered data for the first 100 days: {len(filtered_data)} rows.")

    # Write the filtered data to the database
    filtered_data.to_sql('sop_credit_transaction', con=engine, if_exists="replace", index=False)
    print("Filtered data written to the database successfully.")

except FileNotFoundError as fnf_error:
    print(f"File not found: {fnf_error}")
except pd.errors.ParserError as parser_error:
    print(f"Error parsing CSV file: {parser_error}")
except Exception as e:
    print(f"An error occurred: {e}")
