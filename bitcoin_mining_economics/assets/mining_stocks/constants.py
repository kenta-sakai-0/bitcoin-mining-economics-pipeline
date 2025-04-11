# income statements
LOCAL_INCOME_STATEMENT_FOLDER_PATH = 'temp/mining_stocks/income_statement/{fetch_date}'
GCS_FILE_PATH_TEMPLATE__INCOME_STATEMENT = 'mining_stocks/income_statement/annual/{fetch_date}/{ticker}.ndjson'
BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__TEMP = 'income_statement_annual__temp'
BQ_TABLE_NAME__INCOME_STATEMENTS_ANNUAL__SOURCE = 'income_statement_annual__source'

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