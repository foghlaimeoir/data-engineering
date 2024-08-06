from dagster import AssetKey, file_relative_path
from dagster_dbt import DagsterDbtTranslator, DbtCliResource

dbt_resource = DbtCliResource(project_dir=file_relative_path(__file__, "../dbt_project"))
dbt_parse_invocation = dbt_resource.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "prepared"