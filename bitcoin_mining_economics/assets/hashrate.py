from dagster import asset
from dagster_gcp import GCSResource, BigQueryResource
from dagster import EnvVar

import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

import importlib 
from assets import constants
importlib.reload(constants)

@asset(
    group_name="btc"
)
def btc_hashrate_file(gcs:GCSResource): 
    """
        The raw parquet file for daily bitcoin hashrate. Uses coinmetrics API because it's free
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
    bucket = gcs_client.bucket(EnvVar("BITCOIN_MINING_BUCKET_NAME").get_value())
    
    blob = bucket.blob(constants.GCS_BTC_HASHRATE_FILE_PATH.format(datetime.now().strftime("%Y-%m-%d")))
    blob.upload_from_filename(constants.LOCAL_HASHRATE_FILE_PATH)

@asset(
    deps=["btc_hashrate_file"],
    group_name="btc"
)
def btc_hashrate(bq:BigQueryResource):
    """
        The dataset for bitcoin hashrate. Daily resolution
    """

    # Construct a BigQuery client object.
    with bq.get_client() as bq_client:

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )

        table_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_BTC_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_TABLE_NAME}"
        uri = f"gs://{constants.GCS_BTC_BUCKET_NAME}/{constants.GCS_BTC_HASHRATE_FILE_PATH}"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
    
    os.remove(constants.LOCAL_HASHRATE_FILE_PATH)