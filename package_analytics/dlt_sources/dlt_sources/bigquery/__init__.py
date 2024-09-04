import os
from dataclasses import field

import dlt
from .helpers import get_bigquery_result_ibis
from .queries import build_pypi_query_day
from .settings import START_DATE
from datetime import datetime, timedelta, timezone

GCP_PROJECT = os.getenv("GCP_PROJECT")

# create a timezone aware datetime object for the initial value of dlt's incremental cursor
naive_initial_date = datetime.strptime(START_DATE, "%Y-%m-%d")
aware_initial_date = naive_initial_date.replace(tzinfo=timezone.utc)


@dlt.resource()
def downloads(
        packages: list[str],
        start_date=dlt.sources.incremental(
            "timestamp",
            initial_value=aware_initial_date,
         ),
):

    # generate daily queries to avoid BigQuery response limits
    start_date = start_date.start_value

    current_date = datetime.now(timezone.utc)

    delta = (current_date.date() - start_date.date()).days

    queries = []
    for i in range(delta + 1):
        start_time = start_date + timedelta(days=i)

        # for the last interval set the end time to the current time
        if i == delta:
            end_time = current_date
        else:
            end_time = start_date + timedelta(days=i + 1)

        # Build and append the query
        query_str = build_pypi_query_day(start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                         end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                                         packages=packages)
        queries.append(query_str)

    for query in queries:
        yield get_bigquery_result_ibis(query, GCP_PROJECT)


@dlt.source
def pypi(
        packages: list = field(default_factory=lambda: ["duckdb"]),
):

    return downloads(packages=packages)
