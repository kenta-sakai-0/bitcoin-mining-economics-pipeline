from dagster_gcp import GCSResource, BigQueryResource
from dagster_duckdb import DuckDBResource
from dagster import EnvVar

gcs_resource = GCSResource(
    project=EnvVar("GCP_PROJECT_ID")
)

bq_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT_ID")
)

duckdb_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")
)