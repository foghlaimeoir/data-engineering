from typing import Any
import dlt

from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

from rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)


@dlt.source
def hackernews_search(query: str,) -> Any:
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": f"http://hn.algolia.com/api/v1/",
            "paginator": PageNumberPaginator(
                total_path="nbPages",
            )
        },
        # The default configuration for all resources and their endpoints
        # "resource_defaults": {
        #     "primary_key": "id",
        #     "write_disposition": "merge",
        #     "endpoint": {
        #         "params": {
        #             "per_page": 100,
        #         },
        #     },
        # },
        "resources": [
            {
                "name": "story",
                "endpoint": {
                    "path": "search",
                    # Query parameters for the endpoint
                    "params": {
                        "query": "duckdb",
                        "tags": "story",
                        "numericFilters": "num_comments>=1",
                        # Define `since` as a special parameter
                        # to incrementally load data from the API.
                        # This works by getting the updated_at value
                        # from the previous response data and using this value
                        # for the `since` query parameter in the next request.
                        # "since": {
                        #     "type": "incremental",
                        #     "cursor_path": "updated_at",
                        #     "initial_value": "2024-01-25T11:21:28Z",
                        # },
                    },
                },
            },
            {
                "name": "story_comments",
                "endpoint": {
                    # The placeholder {issue_number} will be resolved
                    # from the parent resource
                    "path": "search?tags=comment,story_{story_id}",
                    "params": {
                        # The value of `issue_number` will be taken
                        # from the `number` field in the `issues` resource
                        "story_id": {
                            "type": "resolve",
                            "resource": "story",
                            "field": "story_id",
                        }
                    },
                }
            }
            #     # Include data from `id` field of the parent resource
            #     # in the child data. The field name in the child data
            #     # will be called `_issues_id` (_{resource_name}_{field_name})
            #     "include_from_parent": ["id"],
        ]
    }

    yield from rest_api_resources(config)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_hackernews",
        destination='duckdb',
        dataset_name="rest_api_data",
    )
    load_info = pipeline.run(hackernews_search("duckdb"))
    print(load_info)
