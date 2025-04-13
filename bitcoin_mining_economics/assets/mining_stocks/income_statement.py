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
def income_statement_annual__files(gcs:GCSResource):
    """
        Source: FMP
    """
    
    fetch_date = datetime.today().strftime('%Y-%m-%d')
    
    # Init GCS client
    gcs_client = gcs.get_client()
    bucket = gcs_client.bucket(project_constants.GCS_BUCKET_NAME)

    for ticker in constants.TICKER_LIST:

        # Get response
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=annual&apikey={EnvVar('FMP_API_KEY').get_value()}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(response.json())
        ndjson = "\n".join(json.dumps(record) for record in response.json())
        
        # Prep GCS
        gcs_file_path = constants.GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT_ANNUAL.format(
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
def income_statement_annual__src(bq:BigQueryResource):
    """
        External table for annual income statements
    """

    # Create dataset if not exists
    with bq.get_client() as bq_client:        
        dataset_id = f"{EnvVar('GCP_PROJECT_ID').get_value()}.{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}"
        bq_client.create_dataset(dataset_id, exists_ok=True,)


    fetch_date = datetime.today().strftime('%Y-%m-%d')
    temp_table_id = f"{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}.{constants.BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__TEMP}"
    source_table_id = f"{constants.BQ_DATASET_NAME__FUNDAMENTALS_SRC}.{constants.BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__SOURCE}"

    query = f"""
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
            uris = ['gs://{project_constants.GCS_BUCKET_NAME}/{constants.GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT_ANNUAL.format(fetch_date=fetch_date, ticker='*')}']
        );

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