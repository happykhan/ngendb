import typer
import logging 
from ngendb.create.create_funcs import create_new_duckdb_from_sqllite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

app = typer.Typer()

@app.command()
def create_db(
    sqlite_db: str = typer.Option("atb.metadata.202408.sqlite", help="Path to the SQLite database", exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True, show_default=True),
    duckdb_db: str = typer.Option("nfbac.atb202408.duckdb", help="Path to the DuckDB database", file_okay=True, dir_okay=False, resolve_path=True, show_default=True),
    merged_csv: str = typer.Option("merged_data.csv", help="Path to save the merged CSV file", file_okay=True, dir_okay=False, resolve_path=True, show_default=True),
):
    """Create a DuckDB database from an SQLite database."""
    create_new_duckdb_from_sqllite(sqlite_db, duckdb_db, merged_csv)

@app.command()
def delete_db(
    duckdb_db: str = typer.Option("nfbac.atb202408.duckdb", help="Path to the DuckDB database", file_okay=True, dir_okay=False, resolve_path=True, show_default=True),
):
    """Delete the DuckDB database file."""
    import os
    if os.path.exists(duckdb_db):
        os.remove(duckdb_db)
        typer.echo(f"Deleted DuckDB database: {duckdb_db}")
    else:
        typer.echo(f"DuckDB database not found: {duckdb_db}")

if __name__ == "__main__":
    app()
