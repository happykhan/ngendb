import duckdb
import pandas as pd
import logging
import os 
import sqlite3
from ngendb.startup.io_utils import zip_csv

def create_new_duckdb_from_sqllite(sqlite_db, duckdb_db, merged_csv, remove_duckdb=False):
    # Remove DuckDB if it exists
    if remove_duckdb and os.path.exists(duckdb_db):
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


def create_duckdb_from_df(duckdb_conn, df, run_table, table_name='atb_tables'):
    try:
        duckdb_conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        duckdb_conn.from_df(df).create(table_name)
        logging.info('Merged DataFrame written to DuckDB as table: {}'.format(table_name))
    except Exception as exc:
        logging.error('Failed to write merged DataFrame to DuckDB: %s', exc)

def create_nf_stats_table(duckdb_path):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.StreamHandler()]
    )
    try:
        duckdb_conn = duckdb.connect(duckdb_path)
        # Try to get the run table
        try:
            run_table = duckdb_conn.execute('SELECT * FROM run').df()
        except Exception as exc:
            logging.error('Could not read run table from DuckDB: %s', exc)
            run_table = pd.DataFrame(columns=['sample_accession', 'run_accession'])
        # Create the nf_stats DataFrame
        nf_stats = pd.DataFrame(columns=[
            'sample_accession', 'run_accession', 'gc_content', 'passed_qc', 'mlst_st', 'mlst_alleles'
        ])
        if not run_table.empty:
            unique_pairs = run_table[['sample_accession', 'run_accession']].drop_duplicates()
            nf_stats = pd.concat([nf_stats, unique_pairs], ignore_index=True, sort=False)
        duckdb_conn.execute('DROP TABLE IF EXISTS nf_stats')
        duckdb_conn.from_df(nf_stats).create('nf_stats')
        logging.info('nf_stats table created in DuckDB with sample_accession and run_accession rows.')
        duckdb_conn.close()
    except Exception as exc:
        logging.error('Failed to create nf_stats table in DuckDB: %s', exc)


def drop_species_rows(duckdb_path, species_to_drop=None, table='atb_tables'):
    """
    Drop rows from all tables in the DuckDB where Species is in the provided list.
    """
    if species_to_drop is None:
        species_to_drop = ["Salmonella diarizonae", "Sarcina perfringens"]
    try:
        duckdb_conn = duckdb.connect(duckdb_path)
        # Check if the 'Species' column exists in any table
        # Delete rows where Species is in the list
        placeholders = ','.join([f'\'{s}\'' for s in species_to_drop])
        sql = f"DELETE FROM {table} WHERE Species IN ({placeholders})"
        duckdb_conn.execute(sql)
        logging.info('Dropped rows from %s where Species in %s', table, species_to_drop)
        duckdb_conn.close()
    except Exception as exc:
        logging.error('Failed to drop rows by Species in DuckDB: %s', exc)