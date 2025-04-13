from dagster import asset
from dagster_gcp import GCSResource, BigQueryResource
from dagster import EnvVar

from google.cloud import bigquery
import os
import kagglehub
import shutil

import importlib 
from assets.bitcoin import constants
importlib.reload(constants)
import project_constants
importlib.reload(project_constants) 

@asset(
    group_name='btc'
)
def btcusd_file(gcs:GCSResource):
    """
        CSV dataset for bitcoin spot price in USD. Source: mczielinski/bitcoin-historical-data from Kaggle 
    """

    # Configure to temp storage location
    os.environ['KAGGLEHUB_CACHE'] = constants.LOCAL_KAGGLEHUB_CACHE

    filepath = kagglehub.dataset_download(
        "mczielinski/bitcoin-historical-data",
        path='btcusd_1-min_data.csv'
    )

    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(project_constants.GCS_BUCKET_NAME)    
    blob = bucket.blob(constants.GCS_BTCUSD_FILE_PATH)
    blob.upload_from_filename(filepath)

    shutil.rmtree(constants.LOCAL_KAGGLEHUB_CACHE) # Kaggle creates folder, so remove the entire thing

@asset(
    deps=["btcusd_file"],
    group_name="btc"
)
def btcusd_src(bq:BigQueryResource):
    """
        Dataset for BTCUSD.
    """

    # Construct a BigQuery client object.
    with bq.get_client() as bq_client:

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
        
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        
        table_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_DATASET_NAME__BTC_SRC}.{constants.BQ_TABLE_NAME__BTCUSD_SRC}"
        uri = f"gs://{project_constants.GCS_BUCKET_NAME}/{constants.GCS_BTCUSD_FILE_PATH}"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result() 