import requests
import json
import pandas as pd

# Fetch the data
response = requests.get("https://mempool.space/api/v1/mining/hashrate/alltime")
data = response.json()

# Extract just the hashrates array
hashrates = data["hashrates"]

# Create DataFrame directly from the hashrates list
df = pd.DataFrame(hashrates)

# Convert timestamp to datetime (optional)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

# Print the result
print(df.head())