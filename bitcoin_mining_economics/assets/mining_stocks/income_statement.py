from dagster import asset, EnvVar
from dagster_gcp import GCSResource, BigQueryResource

import os
import json
import requests
from datetime import datetime 
import shutil

import importlib 
from assets.mining_stocks import constants
from assets.constants import GCS_BUCKET_NAME
importlib.reload(constants)

@asset(
    group_name='mining_stocks'
)
def income_statement_files(gcs:GCSResource):

    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    # Create temporary local income_statement folder
    local_income_statement_folder_path = constants.LOCAL_INCOME_STATEMENT_FOLDER_PATH.format(fetch_date=fetch_date) 
    os.makedirs(local_income_statement_folder_path, exist_ok=True) 
    
    # Init GCS client
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    for ticker in constants.TICKER_LIST:
        url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={EnvVar('ALPHA_VANTAGE_API_KEY').get_value()}"
        r = requests.get(url)
        data = r.json()

        gcs_income_statement_template_file_path = constants.GCS_INCOME_STATEMENT_TEMPLATE_FILE_PATH.format(
            fetch_date=fetch_date,
            ticker=ticker
        )
        blob = bucket.blob(gcs_income_statement_template_file_path)
        
        blob.upload_from_string(
            json.dumps(data), 
            content_type='application/json'
        )

# @asset(
#     group_name='mining_stocks'
# )
# def income_statement_sorc