from dagster_gcp import GCSResource, BigQueryResource
from dagster_dbt import DbtCliResource
from dagster import EnvVar
from project import dbt_project

gcs_resource = GCSResource(
    project=EnvVar("GCP_PROJECT_ID").get_value(),
)

bq_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT_ID").get_value()
)

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)