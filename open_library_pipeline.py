"""Pipeline to ingest data from Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from Open Library REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org",
            # No authentication required for public search API
        },
        "resources": [
            {
                "name": "books_search",
                "endpoint": {
                    "path": "/search.json",
                    "method": "GET",
                    "params": {
                        "q": "harry potter",
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "total_path": "numFound",
                        "maximum_page": 5,  # Limit to 5 pages for demo
                    },
                },
                "primary_key": "key",
                "write_disposition": "replace",
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    dataset_name="open_library_data",
    dev_mode=True,
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
