import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://pokeapi.co/api/v2/",
        "paginator": {
            "type": "json_link",
            "next_url_path": "next",
        },
    },
    "resources": ["pokemon", "ability", "type"],
})

pipeline = dlt.pipeline(
    pipeline_name="pokemon_api",
    destination="duckdb",
    dataset_name="pokemon_data",
)

load_info = pipeline.run(source)
print(load_info)
