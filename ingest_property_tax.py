import requests
import polars as pl  # Polars library
import duckdb  # DuckDB library

# Define the dataset URL
DATASET_URL = "https://opendata.vancouver.ca/api/records/1.0/search/?dataset=property-tax-report&sort=-tax_assessment_year&rows=1000"

# DuckDB database file
DB_PATH = 'vancouver_data.db'

def fetch_data():
    """Fetch data from the Vancouver open data API and return as a Polars DataFrame."""
    try:
        response = requests.get(DATASET_URL)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        records = [record['fields'] for record in data['records']]
        
        # Convert data to Polars DataFrame
        return pl.DataFrame(records)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return pl.DataFrame()

def store_in_duckdb(df, table_name='property_tax'):
    """Store the fetched Polars DataFrame directly in DuckDB."""
    try:
        if not df.is_empty():
            # Use DuckDB connection to directly insert Polars DataFrame
            con = duckdb.connect(DB_PATH)
            
            # Store the Polars DataFrame directly in DuckDB
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            
            print(f"Data successfully ingested into table: {table_name}")
        else:
            print("No data to ingest.")
    except Exception as e:
        print(f"Error storing data in DuckDB: {e}")

def main():
    # Fetch the data using Polars
    df = fetch_data()
    
    # Print sample of fetched data
    print("Sample fetched data:")
    print(df.head())

    # Store the data in DuckDB
    if not df.is_empty():
        store_in_duckdb(df)
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()