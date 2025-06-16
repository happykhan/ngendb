from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from ngendb.assets import mlst, report

all_assets = load_assets_from_modules([mlst, report])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database="./db/nfbac.atb202408.duckdb",  # required
        ),
        "io_manager": DuckDBPandasIOManager(
            database="./db/iomanager.duckdb", schema="mlst"
        ),
    },
    # sensors=[illumina_workflow.unprocessed_illumina_samples_sensor],
    # jobs=[illumina_workflow.unprocessed_illumina_samples_job],
)