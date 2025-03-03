from dagster import asset, AssetExecutionContext
from dagster_gcp import GCSResource, BigQueryResource
from . import constants
from ..partitions import monthly_partition
import requests
from datetime import datetime
import os 
import logging 
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

@asset
def btc_hashrate_file(context: AssetExecutionContext, gcs:GCSResource): 
    """
        The raw parquet file for daily bitcoin hashrate
    """
    # Fetch JSON data from the API
    url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
    params = {
        "assets": "btc",
        "metrics": "HashRate",
        "frequency": "1d"
    }

    response = requests.get(url, params=params).json()
    response_data = response['data']
    df = pd.DataFrame(response_data)
    
    # Typecast
    df['asset'] = df['asset'].astype(str)
    df['time'] = pd.to_datetime(df['time'])
    df['HashRate'] = pd.to_numeric(df['HashRate'])
    df['created_at'] = datetime.now()

    df.to_parquet(
        constants.LOCAL_HASHRATE_FILE_PATH, 
        index=False, 
        coerce_timestamps="us" # https://www.reddit.com/r/bigquery/comments/16aoq0u/parquet_timestamp_to_bq_coming_across_as_int/
    )

    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(os.getenv("BITCOIN_MINING_BUCKET_NAME"))
    blob = bucket.blob(constants.GCS_HASHRATE_TEMPLATE_FILE_PATH.format(datetime.now().strftime("%Y-%m-%d")))
    blob.upload_from_filename(constants.LOCAL_HASHRATE_FILE_PATH)

@asset(
    deps=["btc_hashrate_file"]
)
def btc_hashrate_staging(bq:BigQueryResource):
    """
        The staging dataset for daily bitcoin hashrate
    """
    query = f"""
        create or replace external table {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_STAGING_TABLE_NAME}
        options (
            format="PARQUET",
            uris=["gs://{os.getenv("BITCOIN_MINING_BUCKET_NAME")}/{constants.GCS_HASHRATE_TEMPLATE_FILE_PATH.format(datetime.now().strftime("%Y-%m-%d"))}"]
        )
    """

    with bq.get_client() as bq_client:
        bq_client.query(query)

@asset(
    deps=["btc_hashrate_staging"]
)
def btc_hashrate(bq:BigQueryResource):
    """
        The dataset for daily bitcoin hashrate
    """

    query = f"""
        declare min_date date;
        declare max_date date;

        SET min_date = (
            SELECT MIN(DATE(time))
            FROM {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_STAGING_TABLE_NAME}
        );
        SET max_date = (
            SELECT MAX(DATE(time))
            FROM {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_STAGING_TABLE_NAME}
        );

        create table if not exists {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_TABLE_NAME} (
            `asset` string, 
            `date` date,
            `HashRate` float64,
            `created_at` timestamp 
        )
        partition by `date`
        options (
            require_partition_filter=true
        )
        ;

        delete from {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_TABLE_NAME} h
            where `date` between min_date and max_date
            and `date` IN (select extract(date from time) from {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.btc_hashrate_staging)   
        ;

        insert into {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_TABLE_NAME}
            (select asset, date(`time`) as date, HashRate, created_at 
            from {os.getenv("GCP_PROJECT_ID")}.{constants.BQ_HASHRATE_DATASET_NAME}.{constants.BQ_BTC_HASHRATE_STAGING_TABLE_NAME})
    """

    with bq.get_client() as bq_client:
        bq_client.query(query)