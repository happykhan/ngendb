import duckdb
import logging

CONN = None

def get_conn(db_path: str = "nfbac.atb202408.duckdb"):
    """Connect to the DuckDB database."""
    try:
        global CONN
        if CONN is not None:
            return CONN
        CONN = duckdb.connect("nfbac.atb202408.duckdb")
        return CONN
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB: {e}")
        raise