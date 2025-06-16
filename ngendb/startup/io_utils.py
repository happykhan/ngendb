import gzip
import logging

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