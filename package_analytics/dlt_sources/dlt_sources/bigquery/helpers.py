import os

import ibis
from loguru import logger
import time
from tqdm import tqdm
import pyarrow as pa

from google.cloud import bigquery
from google.oauth2 import service_account


def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Get Big Query client"""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            bigquery_client = bigquery.Client(
                project=project_name, credentials=credentials
            )
            return bigquery_client

        raise EnvironmentError(
            "No valid credentials found for BigQuery authentication."
        )

    except Exception as e:
        logger.error(f"Error creating bigquery client: {e}")
        raise e

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
) -> dict:
    """Get query result from BigQuery and yield rows as dictionaries."""
    try:

        logger.info(f"Running query: {query_str}")

        for row in tqdm(bigquery_client.query(query_str), desc="Loading data..."):
            yield {key: value for key, value in row.items()}

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise

def get_bigquery_result_ibis(
    query_str: str, project_id: str
) -> pa.Table:
    """Get query result from BigQuery and yield rows as an arrow table."""
    try:
        start_time = time.time()

        con = ibis.connect(f"bigquery://{project_id}")

        logger.info(f"Running query: {query_str}")
        table = con.sql(query_str)

        pa_tbl = table.to_pyarrow()

        elapsed_time = time.time() - start_time
        logger.info(f"Query executed and data loaded in {elapsed_time:.2f} seconds")

        return pa_tbl

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise


