from dagster import Definitions, load_assets_from_modules
from resources import gcs_resource, bq_resource, dbt_resource

from assets import dbt
from assets.bitcoin import hashrate, spot
from assets.mining_stocks import income_statement, balance_sheet
bitcoin_assets = load_assets_from_modules([hashrate, spot])
mining_stocks_assets = load_assets_from_modules([income_statement, balance_sheet])
dbt_analytics_assets = load_assets_from_modules([dbt])

from jobs import btc_hashrate_job
all_jobs = [btc_hashrate_job]

from schedules import hashrate_update_schedule
all_schedules = [hashrate_update_schedule]

defs = Definitions(
    assets=[*bitcoin_assets, *mining_stocks_assets, *dbt_analytics_assets],
    resources={
        "gcs": gcs_resource,
        "bq": bq_resource,
        "dbt": dbt_resource
    },
    jobs=all_jobs,
    schedules=all_schedules
)
