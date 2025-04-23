import requests
import json
import os
import pandas as pd  # Import pandas for data manipulation
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Define API details
API_URL = "https://api.api-ninjas.com/v1/inflation?country="
API_KEY = os.environ.get("API_KEY", "default_value")  # Get API key from environment variables
HEADERS = {"X-Api-Key": API_KEY}

def extract_data():
    """Extract data from the Inflation API."""
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        print(response.json())
        return response.json()  # Return the JSON response as a Python object
    else:
        raise Exception(f"API request failed with status code {response.status_code}: {response.text}")


def transform_data(data):
    """Transform the extracted data into a pandas DataFrame."""
    # Convert JSON data to a list of dictionaries
    records = [
    {key: value for key, value in item.items()}
    for item in data
]
    # Create a pandas DataFrame
    return pd.DataFrame(records)

def load_data(df):
    """Load the transformed data into a storage or display."""
    # Display the DataFrame content
    print(df)
    # Save the DataFrame to a CSV file
    df.to_csv("inflation_rates.csv", index=False)
    print("Data saved to inflation_rates.csv")

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