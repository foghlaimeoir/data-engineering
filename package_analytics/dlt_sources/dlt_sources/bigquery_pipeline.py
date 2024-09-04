import os
from datetime import datetime, timedelta

from google.cloud import bigquery
# from google.cloud import storage
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import time
# from .models import PypiJobParameters, FileDownloads
import pyarrow as pa


from dotenv import load_dotenv
from bigquery.models import PypiJobParameters
from bigquery.queries import build_pypi_query_day
from bigquery.helpers import get_bigquery_client
load_dotenv()
import dlt
from tqdm import tqdm


PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"
GCS_BUCKET = "gs://pypi_downloads"



@dlt.source
def bigquery_source(
    params: PypiJobParameters, bigquery_client: bigquery.Client
):

    query_str = build_pypi_query_day(params)
    return dlt.resource(
        get_bigquery_result_arrow,
        name=params.pypi_project,
    )(query_str, bigquery_client)



def get_bigquery_result_arrow(
    query_str: str, bigquery_client: bigquery.Client
) -> pa.Table:
    """Get query result from BigQuery and yield rows as an arrow table."""
    try:
        start_time = time.time()
        logger.info(f"Running query: {query_str}")

        pa_tbl = bigquery_client.query(query_str).to_arrow()

        elapsed_time = time.time() - start_time
        logger.info(f"Query executed and data loaded in {elapsed_time:.2f} seconds")

        yield pa_tbl

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise

def get_bigquery_result_dict(
    query_str: str, bigquery_client: bigquery.Client
) -> pa.Table:
    """Get query result from BigQuery and yield rows as dictionaries."""
    try:

        logger.info(f"Running query: {query_str}")

        for row in tqdm(bigquery_client.query(query_str), desc="Loading data..."):
            yield {key: value for key, value in row.items()}

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise

def get_bigquery_result_connectorx(
    query_str: str, bigquery_client: bigquery.Client
) -> pa.Table:
    """Get query result from BigQuery and yield rows as dictionaries."""
    try:


        for row in tqdm(bigquery_client.query(query_str), desc="Loading data..."):
            yield {key: value for key, value in row.items()}

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


def main(
        # params: PypiJobParameters
):
    params = PypiJobParameters(
        start_date="2024-08-05",
        end_date="2023-11-30",
        pypi_project="duckdb",
        gcp_project='saor-trial'
    )

    bigquery_client = get_bigquery_client(project_name=params.gcp_project)

    data_source = bigquery_source(params, bigquery_client=bigquery_client)

    pipeline = dlt.pipeline(
        pipeline_name=f"bigquery_{params.pypi_project}",
        destination=dlt.destinations.duckdb("pypi_analytics.duckdb"),
        dataset_name="main",
    )

    load_info = pipeline.run(data_source, write_disposition="append")

    print(load_info)



if __name__ == "__main__":
    main()
    # fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))

