from dagster import asset
from dagster_gcp import GCSResource, BigQueryResource
from dagster import EnvVar

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

import importlib 
from assets import constants
importlib.reload(constants)

@asset(
    group_name="btc"
)
def btc_hashrate_file(gcs:GCSResource): 
    """
        The raw csv file daily bitcoin hashrate. Source: mempool.space/api/v1/mining/hashrate
    """

    response = requests.get("https://mempool.space/api/v1/mining/hashrate/100y")
    if response.status_code == 200:
        data = response.json()
    else:
        raise Exception(f"Request failed with status code: {response.status_code}")
    
    hashrates = data["hashrates"]
    df = pd.DataFrame(hashrates)
    df.to_csv(
        constants.LOCAL_HASHRATE_FILE_PATH,
        index=False
    )

    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(constants.BITCOIN_MINING_BUCKET_NAME)
    
    blob = bucket.blob(constants.GCS_BTC_HASHRATE_FILE_PATH.format(datetime.now().strftime("%Y-%m-%d")))
    blob.upload_from_filename(constants.LOCAL_HASHRATE_FILE_PATH)

@asset(
    deps=["btc_hashrate_file"],
    group_name="btc"
)
def btc_hashrate(bq:BigQueryResource):
    """
        Dataset for bitcoin hashrate. Daily resolution
    """

    # Construct a BigQuery client object.
    with bq.get_client() as bq_client:

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )

        table_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_BTC_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_TABLE_NAME}"
        uri = f"gs://{constants.GCS_BUCKET_NAME}/{constants.GCS_BTC_HASHRATE_FILE_PATH}"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
    
    os.remove(constants.LOCAL_HASHRATE_FILE_PATH)

@asset(
    group_name='btc'
)
def block_reward_file(gcs:GCSResource):
    """
        Raw csv for block reward.
    """
    start_date = datetime(2009, 1, 3)
    
    # Halving dates
    halvings = [
        (datetime(2012, 11, 28), 25.0),
        (datetime(2016, 7, 9), 12.5),
        (datetime(2020, 5, 11), 6.25),
        (datetime(2024, 4, 20), 3.125),
        (datetime(2028, 5, 1), 1.5625)
    ]
    
    # Create lists to store data
    dates = []
    block_rewards = []
    
    # Current block reward
    current_reward = 50.0
    
    current_date = start_date
    end_date = datetime.today()

    while current_date < end_date:
        # Check if we need to update block reward
        for halving_date, new_reward in halvings:
            if current_date >= halving_date:
                current_reward = new_reward
        
        # Add to lists
        dates.append(current_date.date())
        block_rewards.append(current_reward)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Create DataFrame
    df = pd.DataFrame({
        'date': dates,
        'block_reward': block_rewards
    })
    
    df.to_csv(
        constants.LOCAL_BLOCK_REWARD_FILE_PATH,
        index=False
    )

    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(constants.GCS_BUCKET_NAME)
    
    blob = bucket.blob(constants.GCS_BLOCK_REWARD_FILE_PATH)
    blob.upload_from_filename(constants.LOCAL_BLOCK_REWARD_FILE_PATH)
    os.remove(constants.LOCAL_BLOCK_REWARD_FILE_PATH)

@asset(
    deps=['block_reward_file'],
    group_name='btc'
)
def block_reward(bq:BigQueryResource):
    """
        Dataset for block reward
    """

    # Construct a BigQuery client object.
    with bq.get_client() as bq_client:

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )

        table_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_BTC_DATASET_NAME}.{constants.BQ_BLOCK_REWARD_TABLE_NAME}"
        uri = f"gs://{constants.GCS_BUCKET_NAME}/{constants.GCS_BLOCK_REWARD_FILE_PATH}"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
    
    