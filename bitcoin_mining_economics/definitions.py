from dagster import Definitions, load_assets_from_modules
from .resources import gcs_resource, bq_resource, duckdb_resource

from .assets import bitcoin
bitcoin_assets = load_assets_from_modules([bitcoin])

from .jobs import btc_hashrate_job
all_jobs = [btc_hashrate_job]

from .schedules import hashrate_update_schedule
all_schedules = [hashrate_update_schedule]

defs = Definitions(
    assets=bitcoin_assets,
    resources={
        "gcs": gcs_resource,
        "bq": bq_resource,
        "duckdb": duckdb_resource
    },
    jobs=all_jobs,
    schedules=all_schedules
)
