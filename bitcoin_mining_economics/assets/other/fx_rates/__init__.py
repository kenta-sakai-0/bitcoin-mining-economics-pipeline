from dagster import asset, EnvVar
from dagster_gcp import GCSResource, BigQueryResource

import json
import requests
from datetime import datetime 

import importlib 
from assets.other.fx_rates import constants
from assets.other.constants import BQ_DATASET_NAME__OTHER_SRC
import project_constants
importlib.reload(project_constants) 

@asset(
    group_name='other'
)
def fx_rates__files(gcs:GCSResource):
    """
        Raw FX rate files for currency pairs such as USDCAD,EURUSD, etc. Source: FMP
    """
    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    # Init GCS client
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(project_constants.GCS_BUCKET_NAME)

    for currency_pair in constants.CURRENCY_PAIRS_LIST:

        # Get response
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={currency_pair}&apikey={EnvVar('FMP_API_KEY').get_value()}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(response.json())
        ndjson = "\n".join(json.dumps(record) for record in response.json())
        
        # Prep GCS
        gcs_file_path = constants.GCS_FILE_PATH_TEMPLATE__FX_RATES.format(
            fetch_date=fetch_date,
            currency_pair=currency_pair
        )
        blob = bucket.blob(gcs_file_path)
        
        # Upload to GCS
        blob.upload_from_string(
            ndjson, 
            content_type='application/json'
        )

@asset(
    group_name='other',
    deps=['fx_rates__files']
)
def fx_rates__src(bq:BigQueryResource):
    """
        Source table of FX rate for currency pairs such as USDCAD, EURUSD, etc
    """
    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    with bq.get_client() as bq_client: 
        
        # Create dataset if not exists
        dataset_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{BQ_DATASET_NAME__OTHER_SRC}"
        bq_client.create_dataset(dataset_id, exists_ok=True)
        
        temp_table_id = f"{BQ_DATASET_NAME__OTHER_SRC}.{constants.BQ_TABLE_NAME__FX_RATES_TEMP}"
        source_table_id =f"{BQ_DATASET_NAME__OTHER_SRC}.{constants.BQ_TABLE_NAME__FX_RATES_SRC}"
        query = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{temp_table_id}`
                (
                    symbol STRING, 
                    date DATE,
                    open FLOAT64,
                    high  FLOAT64,
                    low FLOAT64,
                    close FLOAT64,
                    volume FLOAT64,
                    change FLOAT64,
                    changePercent FLOAT64,
                    vwap FLOAT64,

                )
            OPTIONS (
                format = 'JSON',
                uris = ['gs://{project_constants.GCS_BUCKET_NAME}/{constants.GCS_FILE_PATH_TEMPLATE__FX_RATES.format(fetch_date=fetch_date, currency_pair='*')}']
            );
          
            CREATE TABLE IF NOT EXISTS {source_table_id} AS (
                SELECT *, CURRENT_TIMESTAMP() AS created_at
                FROM {temp_table_id}
            );

            # Insert new rows
            INSERT INTO {source_table_id}
            SELECT *, CURRENT_TIMESTAMP() AS created_at 
            FROM {temp_table_id} t
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {source_table_id} s
                WHERE s.symbol = t.symbol AND s.date = t.date
            );

            DROP TABLE {temp_table_id}
        """
        
        query_job = bq_client.query(query)
        query_job.result()