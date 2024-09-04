import os

import ibis

import dlt

from models import PypiJobParameters

from tqdm import tqdm

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"



def build_pypi_query(
    params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    # Query the public PyPI dataset from BigQuery
    # /!\ This is a large dataset, filter accordingly /!\

    if params.end_date is None:
        return f"""
        SELECT *
        FROM
            `{pypi_public_dataset}`
        WHERE
            project = '{params.pypi_project}'
            AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}")
            AND {params.timestamp_column} < TIMESTAMP_ADD(TIMESTAMP("{params.start_date}"), INTERVAL 1 DAY)
        """
    else:
        return f"""
        SELECT *
        FROM
            `{pypi_public_dataset}`
        WHERE
            project = '{params.pypi_project}'
            AND timestamp >= TIMESTAMP("{params.start_date}")
            AND timestamp < TIMESTAMP("{params.end_date}")
        """

@dlt.resource(table_name="custom_table", write_disposition="append", max_table_nesting=2)
def custom_query(
    # updated_at=dlt.sources.incremental("timestamp", initial_value="1970-01-01T00:00:00Z")
):
    # create sql alchemy engine
    # engine = create_engine('your_database_connection_string')

    con = ibis.bigquery.connect(
        project_id='saor-trial',
        dataset_id='bigquery-public-data.pypi',
    )

    test_params = PypiJobParameters(
        pypi_project='ibis-framework',
        # timestamp_column='timestamp',
        start_date='2020-01-01',
        end_date='2020-01-02'
    )
    # write your custom SQL query
    query = build_pypi_query(test_params)

    # execute the query and yield the results
    result = con.sql(query)
    result = result.to_pyarrow().to_pylist()

    for row in result:
        print(row)
        # yield row
        yield dict(row)


# # create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="custom_query_incremental",
    destination="duckdb",
    dataset_name="pypi",
)

# run the pipeline
load_info = pipeline.run(custom_query)

# custom_query()

