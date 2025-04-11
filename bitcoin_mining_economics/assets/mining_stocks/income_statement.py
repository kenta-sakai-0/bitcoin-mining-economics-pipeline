from dagster import asset, EnvVar
from dagster_gcp import GCSResource, BigQueryResource

import os
import json
import requests
from datetime import datetime 
import shutil
import pandas as pd

import importlib 
from assets.mining_stocks import constants
from assets.constants import GCS_BUCKET_NAME
importlib.reload(constants)

@asset(
    group_name='mining_stocks'
)
def income_statement_annual__files(gcs:GCSResource):
    """
        Source: FMP
    """

    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    # Create temporary local income_statement folder
    local_income_statement_folder_path = constants.LOCAL_INCOME_STATEMENT_FOLDER_PATH.format(fetch_date=fetch_date) 
    os.makedirs(local_income_statement_folder_path, exist_ok=True) 
    
    # Init GCS client
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)

    for ticker in constants.TICKER_LIST:

        # Get response
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=annual&apikey={EnvVar('FMP_API_KEY').get_value()}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(response.json())
        ndjson = "\n".join(json.dumps(record) for record in response.json())
        
        # Prep GCS
        gcs_file_path = constants.GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT.format(
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
    deps=['income_statement_annual__files']
)
def income_statement_annual__temp(bq:BigQueryResource):
    """
        External table for annual income statements
    """

    fetch_date = datetime.today().strftime('%Y-%m-%d')
    query = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{EnvVar('DBT_DATASET_NAME').get_value()}.{constants.BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__TEMP}`
            (
            date DATE,
            symbol STRING,
            reportedCurrency STRING,
            cik STRING,
            fillingDate DATE,
            acceptedDate TIMESTAMP,
            calendarYear STRING,
            period STRING,
            revenue FLOAT64,
            costOfRevenue FLOAT64,
            grossProfit FLOAT64,
            grossProfitRatio FLOAT64,
            researchAndDevelopmentExpenses FLOAT64,
            generalAndAdministrativeExpenses FLOAT64,
            sellingAndMarketingExpenses FLOAT64,
            sellingGeneralAndAdministrativeExpenses FLOAT64,
            otherExpenses FLOAT64,
            operatingExpenses FLOAT64,
            costAndExpenses FLOAT64,
            interestIncome FLOAT64,
            interestExpense FLOAT64,
            depreciationAndAmortization FLOAT64,
            ebitda FLOAT64,
            ebitdaratio FLOAT64,
            operatingIncome FLOAT64,
            operatingIncomeRatio FLOAT64,
            totalOtherIncomeExpensesNet FLOAT64,
            incomeBeforeTax FLOAT64,
            incomeBeforeTaxRatio FLOAT64,
            incomeTaxExpense FLOAT64,
            netIncome FLOAT64,
            netIncomeRatio FLOAT64,
            eps FLOAT64,
            epsdiluted FLOAT64,
            weightedAverageShsOut FLOAT64,
            weightedAverageShsOutDil FLOAT64,
            link STRING,
            finalLink STRING
        )
        OPTIONS (
        format = 'JSON',
        uris = ['gs://bitcoin-mining-economics/{constants.GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT.format(fetch_date=fetch_date, ticker='*')}']
        );
    """

    with bq.get_client() as bq_client:
        query_job = bq_client.query(query)
        query_job.result()

@asset(
    group_name='mining_stocks',
    deps=['income_statement_annual__temp']
)
def income_statement_annual__source(bq:BigQueryResource):
    """
        Source table for annual income statement
    """

    source_table_id = f"{EnvVar('DBT_DATASET_NAME').get_value()}.{constants.BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__SOURCE}"
    temp_table_id = f"{EnvVar('DBT_DATASET_NAME').get_value()}.{constants.BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__TEMP}"

    query = f"""
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
    """

    with bq.get_client() as bq_client:
        query_job = bq_client.query(query)
        query_job.result()