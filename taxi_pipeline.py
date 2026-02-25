"""Pipeline to ingest NYC taxi data from REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_rest_api_source():
    """Define dlt resources from NYC taxi REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net",
            # No authentication required for this public API
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    "path": "/data_engineering_zoomcamp_api",
                    "method": "GET",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "stop_after_empty_page": True,  # Stop when empty array returned
                        "total_path": None,  # API doesn't return total pages
                    },
                },
                "primary_key": [
                    "Trip_Pickup_DateTime",
                    "Trip_Dropoff_DateTime",
                    "Start_Lat",
                    "Start_Lon",
                ],
                "write_disposition": "append",
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    dev_mode=True,
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201
