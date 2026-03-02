"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from REST API endpoints.  No authentication is needed
    for the Open Library public API."""
    config: RESTAPIConfig = {
        "client": {
            # Open Library is a public API; no authentication required for basic reads
            "base_url": "https://openlibrary.org/",
        },
        # search for all books by Nassim Taleb using the Open Library search API
        # this endpoint is paginated with offset/limit; we'll use an offset paginator
        "resources": [
            {
                "name": "taleb_books",
                "endpoint": {
                    "path": "search.json",
                    "method": "GET",
                    "params": {
                        "author": "Nassim Taleb",
                        # request a reasonable page size; max seems to be 100
                        "limit": 100
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "offset",
                        "offset": 0,
                        "limit": 100,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        # the JSON response contains `numFound` to indicate total
                        "total_path": "numFound"
                    }
                }
            }
        ],
        # no shared defaults required for this simple example
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
