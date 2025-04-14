CURRENCY_PAIRS_LIST = [
    "USDCAD","EURUSD","GBPUSD","USDJPY","AUDUSD","USDCHF","NZDUSD","USDCNY","USDMXN","USDINR"
]
GCS_FILE_PATH_TEMPLATE__FX_RATES = 'fx_rates/{fetch_date}/{currency_pair}.ndjson'
BQ_TABLE_NAME__FX_RATES_TEMP= 'fx_rates_temp'
BQ_TABLE_NAME__FX_RATES_SRC = 'fx_rates_src'