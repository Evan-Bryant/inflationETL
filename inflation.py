from pyspark.sql import SparkSession
import requests
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("InflationRatesETL") \
    .getOrCreate()

# Define API details
API_URL = "https://api.api-ninjas.com/v1/inflation"
API_KEY = os.e  # Replace with your actual API key
HEADERS = {"X-Api-Key": API_KEY}

def extract_data():
    """Extract data from the InflationRates API."""
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")

def transform_data(data):
    """Transform the extracted data into a Spark DataFrame."""
    # Convert JSON data to a list of dictionaries
    records = [{"country": item["country"], "inflation_rate": item["inflation_rate"]} for item in data]
    # Create a Spark DataFrame
    return spark.createDataFrame(records)

def load_data(df):
    """Load the transformed data into a storage or display."""
    # Show the DataFrame content (can be replaced with actual storage logic)
    df.show()

if __name__ == "__main__":
    # ETL Process
    try:
        # Extract
        raw_data = extract_data()
        # Transform
        transformed_df = transform_data(raw_data)
        # Load
        load_data(transformed_df)
    except Exception as e:
        print(f"ETL process failed: {e}")