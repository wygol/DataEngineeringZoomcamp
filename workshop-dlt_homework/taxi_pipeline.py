import dlt
from dlt.sources.rest_api import rest_api_source


@dlt.source
def taxi_data_source():
    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
            },
            "resources": [
                {
                    "name": "taxi_data",
                    "endpoint": {
                        "path": "data_engineering_zoomcamp_api",
                        "paginator": {
                            "type": "page_number",
                            "page_param": "page",
                            "base_page": 1,
                            "total_path": None,
                            "stop_after_empty_page": True,
                        },
                    },
                }
            ],
        }
    )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
        progress="log",
    )
    load_info = pipeline.run(taxi_data_source())
    print(load_info)
