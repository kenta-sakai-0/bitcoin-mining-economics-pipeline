from dagster_gcp import GCSResource, BigQueryResource
from dagster import EnvVar

gcs_resource = GCSResource(
    project=EnvVar("GCP_PROJECT_ID").get_value(),
)

bq_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT_ID").get_value()
)