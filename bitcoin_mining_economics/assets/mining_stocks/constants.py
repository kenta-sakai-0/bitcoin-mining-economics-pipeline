# income statements
GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT_ANNUAL = 'mining_stocks/income_statement/annual/{fetch_date}/{ticker}.ndjson'
# GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT_QUARTERLY = 'mining_stocks/income_statement/quarterly/{fetch_date}/{ticker}.ndjson'
BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__TEMP = 'income_statement_annual_temp'
BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__SOURCE = 'income_statement_annual_src'

# balance sheet
GCS_FILE_PATH_TEMPLATE__BALANCE_SHEET_ANNUAL = 'mining_stocks/balance_sheet/annual/{fetch_date}/{ticker}.ndjson'
BQ_TABLE_NAME__BALANCE_SHEETS_ANNUAL__TEMP = 'balance_sheet_annual_temp'
BQ_TABLE_NAME__BALANCE_SHEETS__ANNUAL__SOURCE = 'balance_sheet_annual_src'

BQ_DATASET_NAME__FUNDAMENTALS_SRC = 'fundamentals_src'

TICKER_LIST = [
    'IREN',
    'CLSK',
    'BITF',
    'CORZ',
    'CIFR',
    'MARA',
    'BTDR',
    'BTBT',
    'RIOT',
    'WULF', 
    'HUT'
]