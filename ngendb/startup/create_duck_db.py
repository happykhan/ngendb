"""
Script to read the SQLite database and create a DuckDB database,
excluding all metadata tables (those starting with 'ena_').
"""

import sqlite3
import duckdb
import os
import pandas as pd
import logging
import argparse
import gzip

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

def zip_csv(input_csv):
    """
    Compress a CSV file using gzip.
    """
    # Gzip the merged CSV
    try:
        with open(input_csv, 'rb') as f_in, gzip.open(input_csv + '.gz', 'wb') as f_out:
            f_out.writelines(f_in)
        logging.info('Gzipped merged CSV written to %s.gz', input_csv)
    except Exception as exc:
        logging.error('Failed to gzip merged CSV: %s', exc)        

def create_duckdb_from_df(duckdb_conn, df, run_table, table_name='atb_tables'):
    try:
        duckdb_conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        duckdb_conn.from_df(df).create(table_name)
        logging.info('Merged DataFrame written to DuckDB as table: {}'.format(table_name))
    except Exception as exc:
        logging.error('Failed to write merged DataFrame to DuckDB: %s', exc)


def main(args):
    sqlite_db = args.sqlite
    duckdb_db = args.duckdb
    merged_csv = args.merged_csv

    # Remove DuckDB if it exists
    if os.path.exists(duckdb_db):
        os.remove(duckdb_db)

    # Connect to SQLite and DuckDB
    sqlite_conn = sqlite3.connect(sqlite_db)
    duckdb_conn = duckdb.connect(duckdb_db)

    logging.info('Read all relevant tables into DataFrames')
    # Read all relevant tables into DataFrames
    assembly = pd.read_sql_query('SELECT * FROM assembly', sqlite_conn)
    assembly_stats = pd.read_sql_query('SELECT * FROM assembly_stats', sqlite_conn)
    checkm2 = pd.read_sql_query('SELECT * FROM checkm2', sqlite_conn)
    run = pd.read_sql_query('SELECT * FROM run', sqlite_conn)
    sylph = pd.read_sql_query('SELECT * FROM sylph', sqlite_conn)

    logging.info('Merging tables into a single DataFrame')
    # Merge in order: assembly -> assembly_stats -> checkm2 -> run -> sylph (all on sample_accession)
    merged = assembly.merge(assembly_stats, on='sample_accession', how='left')
    merged = merged.merge(checkm2, on='sample_accession', how='left', suffixes=('', '_checkm2'))
    merged = merged.merge(run, on='sample_accession', how='left', suffixes=('', '_run'))
    merged = merged.merge(sylph, on='sample_accession', how='left', suffixes=('', '_sylph'))

    logging.info('Merged DataFrame shape: %s', merged.shape)
    create_duckdb_from_df(duckdb_conn, merged, run)
    logging.info('Merged DataFrame written to DuckDB')
    # Save the merged DataFrame to CSV
    logging.info('Saving merged DataFrame to CSV: %s', merged_csv)
    merged.to_csv(merged_csv, index=False)
    logging.info('Merged DataFrame saved to %s', merged_csv)
    zip_csv(merged_csv)
    logging.info('Merged DataFrame gzipped and saved')
    # Close connections
    sqlite_conn.close()
    duckdb_conn.close()


if __name__ == "__main__":
    # Argument parsing
    parser = argparse.ArgumentParser(description='Convert SQLite AllTheBacteria DB to DuckDB, and merge tables.')
    parser.add_argument('--sqlite', type=str, default='atb_db/atb.metadata.202408.sqlite', help='Path to input SQLite database')
    parser.add_argument('--duckdb', type=str, default='nfbac.atb202408.duckdb', help='Path to output DuckDB database')
    parser.add_argument('--merged_csv', type=str, default='merged_atb_tables.csv', help='Path to output merged CSV')
    args = parser.parse_args()    
    main(args)

