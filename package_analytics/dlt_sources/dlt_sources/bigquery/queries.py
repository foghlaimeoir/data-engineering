from datetime import timedelta, datetime

from .settings import PYPI_PUBLIC_DATASET, PYPI_PROJECTS
from .models import PypiJobParameters

def build_pypi_query_range(
    params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    # Query the public PyPI dataset from BigQuery
    # /!\ This is a large dataset, filter accordingly /!\
    return f"""
    SELECT *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND timestamp >= TIMESTAMP("{params.start_date}")
        AND timestamp < TIMESTAMP("{params.end_date}")
    """



def build_pypi_query_day(
        start_time: str,
        end_time: str,
        packages: list,
        pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    # Query the public PyPI dataset from BigQuery
    # /!\ This is a large dataset, filter accordingly /!\

    in_statement = ', '.join([f"'{package}'" for package in packages])

    return f"""
    SELECT *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project IN ({in_statement})
        AND timestamp >= '{start_time}'
        AND timestamp < '{end_time}'
    """