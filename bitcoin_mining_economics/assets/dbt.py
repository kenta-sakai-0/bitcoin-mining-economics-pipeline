import dagster as dg
from dagster_dbt import DbtCliResource, DagsterDbtTranslator,dbt_assets

from project import dbt_project

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if resource_type == "source":
            return dg.AssetKey(f"{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
        
@dbt_assets(
    manifest=dbt_project.manifest_path
)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
