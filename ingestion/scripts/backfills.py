import requests
from datetime import datetime
import pandas as pd
import os

# Initial API endpoint and parameters
url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
params = {
    "assets": "btc",
    "metrics": "HashRate",
    "frequency": "1d"
}

all_data = []

# Loop through paginated API responses until exhausted
while url:
    response = requests.get(url, params=params).json()
    print(f"Fetching data from: {url}")
    all_data.extend(response.get('data', []))
    
    # Get the next page URL and clear params for subsequent requests
    url = response.get('next_page_url')
    params = {}

# Convert aggregated data to a DataFrame
df = pd.DataFrame(all_data)

# Typecasting
df['asset'] = df['asset'].astype(str)
df['time'] = pd.to_datetime(df['time'])
df['HashRate'] = pd.to_numeric(df['HashRate'])
df['created_at'] = datetime.now()

# Extract the year from the timestamp to use for file naming
df['year'] = df['time'].dt.year

# Create output directory if it doesn't exist
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Group the data by year and write each group to a separate Parquet file
for year, group in df.groupby('year'):
    file_name = f"hashrate_{year}.parquet"
    output_path = os.path.join(output_dir, file_name)
    group.to_parquet(output_path, index=False, coerce_timestamps="us")
    print(f"Saved {output_path}")
