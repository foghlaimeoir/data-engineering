from pytrends.request import TrendReq
from datetime import datetime, timezone
import time
from loguru import logger
import dlt
from settings import START_DATE

# create a timezone aware datetime object for the initial value of dlt's incremental cursor
naive_initial_date = datetime.strptime(START_DATE, "%Y-%m-%d")
aware_initial_date = naive_initial_date.replace(tzinfo=timezone.utc)

@dlt.resource(write_disposition = "append")
def interest_over_time(
        orchestration_tools: list[str],
        start_date=dlt.sources.incremental("date", initial_value=aware_initial_date),
):
    pytrend = TrendReq()
    # for tool in orchestration_tools:
    attempts = 0
    max_attempts = 5  # Set a maximum number of attempts to avoid infinite loops
    while attempts < max_attempts:

        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = start_date.start_value.strftime("%Y-%m-%d")

            if start_date == end_date:
                logger.info(f"Data last retrieved today. Exiting...")
                break

            timeframe = f'{start_date} {end_date}'
            pytrend.build_payload(kw_list=orchestration_tools, timeframe=timeframe, geo='')
            data_df = pytrend.interest_over_time()

            data_df.reset_index(inplace=True)
            data_df = data_df.drop(columns=['isPartial'])
            data_df = data_df.melt(id_vars=['date'], var_name='tool', value_name='popularity')

            yield data_df
            break  # Successfully fetched data, exit the retry loop

        except Exception as e:
            logger.info(
                f"Encountered an error fetching data for {orchestration_tools}: {e}. Attempt {attempts + 1}/{max_attempts}. Retrying...")
            attempts += 1
            time.sleep(100)  # Wait before retrying


@dlt.source()
def google_trends(
        orchestration_tools: list[str]
):
    return interest_over_time(orchestration_tools=orchestration_tools)
