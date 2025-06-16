import duckdb
import pandas as pd
import argparse
import logging

# Drop rows from db where Species is  in ["Salmonella diarizonae", "Sarcina perfringens"]


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

def main(args):
    if args.rebuild_table:
        create_nf_stats_table(args.duckdb)
    drop_species_rows(args.duckdb)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create extra nf_stats table in DuckDB.')
    parser.add_argument('--duckdb', type=str, default='nfbac.atb202408.duckdb', help='Path to DuckDB database')
    parser.add_argument('--rebuild_table', action='store_true', help='Rebuild the nf_stats table')
    args = parser.parse_args()
    main(args)
