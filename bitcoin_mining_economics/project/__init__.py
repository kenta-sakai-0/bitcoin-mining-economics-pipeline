# from pathlib import Path

from dagster_dbt import DbtProject

dbt_project = DbtProject(
  project_dir='analytics'
)
dbt_project.prepare_if_dev()