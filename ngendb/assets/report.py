import pandas as pd
from dagster import asset, get_dagster_logger, Definitions, AssetExecutionContext,  RunRequest, sensor
from dagster_duckdb import DuckDBResource
import os 

@asset
def species_report(
    duckdb: DuckDBResource,
) -> None:
    """Generate a report of species counts in the DuckDB database and write to a CSV file."""
    logger = get_dagster_logger()
    # Make an output directory if it doesn't exist
    output_dir = "outputs/reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    with duckdb.get_connection() as conn:
        query = """
        SELECT species, COUNT(*) AS count
        FROM atb_tables
        GROUP BY species
        HAVING COUNT(*) >= 1
        ORDER BY count DESC;
        """
        df = conn.execute(query).df()            
        if df.empty:
            logger.info("No species found with count >= 1.")
            # Write empty file with headers
            df = pd.DataFrame(columns=["species", "count"])
        else:
            logger.info(f"Species report generated with {len(df)} entries.")
        output_path = os.path.join(output_dir, "species_report.csv")
        df.to_csv(output_path, index=False)
        logger.info(f"Species report written to {output_path}.")

@asset
def duckdb_explain_report(
    duckdb: DuckDBResource,
) -> None:
    """Generate a report of the DuckDB database schema using EXPLAIN and write to a CSV file."""
    logger = get_dagster_logger()
    output_dir = "outputs/reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    with duckdb.get_connection() as conn:
        # Get all table names
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()
        explain_rows = []
        for (table_name,) in tables:
            explain_query = f"EXPLAIN SELECT * FROM {table_name};"
            explain_df = conn.execute(explain_query).df()
            explain_df.insert(0, 'table', table_name)
            explain_rows.append(explain_df)
        if explain_rows:
            result_df = pd.concat(explain_rows, ignore_index=True)
        else:
            result_df = pd.DataFrame(columns=["table", "explain_output"])
        output_path = os.path.join(output_dir, "duckdb_explain_report.csv")
        result_df.to_csv(output_path, index=False)
        logger.info(f"DuckDB EXPLAIN report written to {output_path}.")

defs = Definitions(assets=[species_report, duckdb_explain_report])

