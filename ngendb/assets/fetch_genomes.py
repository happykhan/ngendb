import os
import duckdb
import hashlib
from dagster import asset, get_dagster_logger
from dagster_duckdb import DuckDBResource
import sys
import subprocess
import importlib.util
import ngendb.util.atb_util as get_assembly_url

def create_genomes_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS genomes (
            sample_accession TEXT PRIMARY KEY,
            file_path TEXT,
            md5 TEXT
        );
    ''')

@asset
def fetch_and_register_genomes(duckdb: DuckDBResource, sample_ids: list[str]) -> None:
    """Download genomes from ATB S3, compute md5, and register in DuckDB."""
    logger = get_dagster_logger()
    output_dir = "outputs/genomes"
    os.makedirs(output_dir, exist_ok=True)
    with duckdb.get_connection() as conn:
        create_genomes_table(conn)
        for sample_id in sample_ids:
            url = get_assembly_url(sample_id)
            local_path = os.path.join(output_dir, f"{sample_id}.fa.gz")
            # Download genome if not already present
            if not os.path.exists(local_path):
                logger.info(f"Downloading {url} to {local_path}")
                subprocess.run(["wget", "-O", local_path, url], check=True)
            # Submit md5sum as a slurm job
            slurm_script = f"#!/bin/bash\nmd5sum {local_path} | awk '{{print $1}}' > {local_path}.md5"
            slurm_path = os.path.join(output_dir, f"md5_{sample_id}.sh")
            with open(slurm_path, "w") as f:
                f.write(slurm_script)
            subprocess.run(["sbatch", slurm_path], check=True)
            # Read md5 if already computed
            md5_path = f"{local_path}.md5"
            if os.path.exists(md5_path):
                with open(md5_path) as f:
                    md5 = f.read().strip()
            else:
                md5 = None
            # Insert or update the record in DuckDB
            conn.execute('''
                INSERT OR REPLACE INTO genomes (sample_accession, file_path, md5)
                VALUES (?, ?, ?)
            ''', (sample_id, local_path, md5))
            logger.info(f"Registered {sample_id} in DuckDB.")
