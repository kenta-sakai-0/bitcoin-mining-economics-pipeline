from dagster import Definitions, load_assets_from_modules

from .assets import bitcoin
from .resources import gcs_resource, bq_resource, duckdb_resource
bitcoin_assets = load_assets_from_modules([bitcoin])

defs = Definitions(
    assets=bitcoin_assets,
    resources={
        "gcs": gcs_resource,
        "bq": bq_resource,
        "duckdb": duckdb_resource
    }
)
