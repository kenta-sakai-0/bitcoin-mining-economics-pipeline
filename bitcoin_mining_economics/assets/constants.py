LOCAL_HASHRATE_FILE_PATH = "temp/hashrate.csv"
LOCAL_BTCUSD_FILE_PATH = "temp/btcusd.csv"
LOCAL_KAGGLEHUB_CACHE='./temp/kaggle'

# GCS
GCS_BUCKET_NAME = 'bitcoin-mining-economics'
GCS_BTC_HASHRATE_FILE_PATH = "hashrate/raw/hashrate.csv"
GCS_BTCUSD_FILE_PATH = 'btcusd/btcusd.csv'

# Datasets
BQ_BTC_DATASET_NAME = 'btc'
BQ_BTC_STAGING_DATASET_NAME = "btc_staging"

# Tables
BQ_BTCUSD_TABLE_NAME='btcusd'
BQ_BTC_HASHRATE_TABLE_NAME = "hashrate"

KAGGLEHUB_CACHE='/temp'
DATE_FORMAT = "%Y-%m-%d"
START_DATE = "2020-01-01"

# Block rewards 
LOCAL_BLOCK_REWARD_FILE_PATH = 'temp/block_reward.csv'
GCS_BLOCK_REWARD_FILE_PATH = 'block_reward/block_reward.csv'
BQ_BLOCK_REWARD_TABLE_NAME = 'block_reward.csv'