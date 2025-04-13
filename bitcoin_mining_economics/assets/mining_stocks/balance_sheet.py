from dagster import asset, EnvVar
from dagster_gcp import GCSResource, BigQueryResource

import json
import requests
from datetime import datetime 

import importlib 
from assets.mining_stocks import constants
import project_constants
importlib.reload(project_constants) 

@asset(
    group_name='mining_stocks'
)
def balance_sheet_annual__files(gcs:GCSResource):
    """
        Source: FMP
    """

    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    # Init GCS client
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(project_constants.GCS_BUCKET_NAME)

    for ticker in constants.TICKER_LIST:

        # Get response
        url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?period=annual&apikey={EnvVar('FMP_API_KEY').get_value()}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(response.json())
        ndjson = "\n".join(json.dumps(record) for record in response.json())
        
        # Prep GCS
        gcs_file_path = constants.GCS_FILE_PATH_TEMPLATE__BALANCE_SHEET_ANNUAL.format(
            fetch_date=fetch_date,
            ticker=ticker
        )
        blob = bucket.blob(gcs_file_path)
        
        # Upload to GCS
        blob.upload_from_string(
            ndjson, 
            content_type='application/json'
        )

@asset(
    group_name='mining_stocks',
    deps=['balance_sheet_annual__files']
)
def balance_sheets_annual_src(bq:BigQueryResource):
    """
        External table for annual income statements
    """

    # Create dataset if not exists
    with bq.get_client() as bq_client:        
        dataset_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}"
        bq_client.create_dataset(dataset_id, exists_ok=True,)


    temp_table_id = f"{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}.{constants.BQ_TABLE_NAME__BALANCE_SHEETS_ANNUAL__TEMP}"
    source_table_id = f"{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}.{constants.BQ_TABLE_NAME__BALANCE_SHEETS__ANNUAL__SOURCE}"

    fetch_date = datetime.today().strftime('%Y-%m-%d')
    query = f"""
        # create temp table
        CREATE OR REPLACE EXTERNAL TABLE `{temp_table_id}`
            (
            date DATE,
            symbol STRING,
            reportedCurrency STRING,
            cik STRING,
            fillingDate DATE,
            acceptedDate TIMESTAMP,
            calendarYear STRING,
            period STRING,
            cashAndCashEquivalents FLOAT64,
            shortTermInvestments FLOAT64,
            cashAndShortTermInvestments FLOAT64,
            netReceivables FLOAT64,
            inventory FLOAT64,
            otherCurrentAssets FLOAT64,
            totalCurrentAssets FLOAT64,
            propertyPlantEquipmentNet FLOAT64,
            goodwill FLOAT64,
            intangibleAssets FLOAT64,
            goodwillAndIntangibleAssets FLOAT64,
            longTermInvestments FLOAT64,
            taxAssets FLOAT64,
            otherNonCurrentAssets FLOAT64,
            totalNonCurrentAssets FLOAT64,
            otherAssets FLOAT64,
            totalAssets FLOAT64,
            accountPayables FLOAT64,
            shortTermDebt FLOAT64,
            taxPayables FLOAT64,
            deferredRevenue FLOAT64,
            otherCurrentLiabilities FLOAT64,
            totalCurrentLiabilities FLOAT64,
            longTermDebt FLOAT64,
            deferredRevenueNonCurrent FLOAT64,
            deferredTaxLiabilitiesNonCurrent FLOAT64,
            otherNonCurrentLiabilities FLOAT64,
            totalNonCurrentLiabilities FLOAT64,
            otherLiabilities FLOAT64,
            capitalLeaseObligations FLOAT64,
            totalLiabilities FLOAT64,
            preferredStock FLOAT64,
            commonStock FLOAT64,
            retainedEarnings FLOAT64,
            accumulatedOtherComprehensiveIncomeLoss FLOAT64,
            othertotalStockholdersEquity FLOAT64,
            totalStockholdersEquity FLOAT64,
            totalEquity FLOAT64,
            totalLiabilitiesAndStockholdersEquity FLOAT64,
            minorityInterest FLOAT64,
            totalLiabilitiesAndTotalEquity FLOAT64,
            totalInvestments FLOAT64,
            totalDebt FLOAT64,
            netDebt FLOAT64,
            link STRING,
            finalLink STRING
        )
        OPTIONS (
        format = 'JSON',
        uris = ['gs://{project_constants.GCS_BUCKET_NAME}/{constants.GCS_FILE_PATH_TEMPLATE__BALANCE_SHEET_ANNUAL.format(fetch_date=fetch_date, ticker='*')}']
        );

        # Create source table if it doen't exist
        CREATE TABLE IF NOT EXISTS {source_table_id} AS (
            SELECT * FROM {temp_table_id}
        );

        # Insert new rows
        INSERT INTO {source_table_id}
        SELECT * FROM {temp_table_id} t
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {source_table_id} s
            WHERE s.symbol = t.symbol AND s.date = t.date
        );

        DROP TABLE {temp_table_id}
    """

    with bq.get_client() as bq_client:
        query_job = bq_client.query(query)
        query_job.result()