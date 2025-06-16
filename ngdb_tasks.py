import typer
import duckdb
from ngendb import get_conn
import logging 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

app = typer.Typer()

CONN = None

@app.command()
def describe_db(output: str = typer.Option(None, help="Output file for DB schema description")):
    """Write a description of the DuckDB database: list all tables, columns, and types."""

    with get_conn() as conn:
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        output_lines = []
        for (table_name,) in tables:
            output_lines.append(f"Table: {table_name}\n")
            columns = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            output_lines.append("column\ttype\n")
            for col in columns:
                output_lines.append(f"{col[1]}\t{col[2]}\n")
            output_lines.append("\n")
        output_str = ''.join(output_lines)
        if output:
            with open(output, 'w', encoding='utf-8') as out:
                out.write(output_str)
            typer.echo(f"DuckDB schema description written to {output}")
        else:
            typer.echo(output_str)

@app.command()
def export_table(
    table: str = typer.Option(..., help="Table to export",
        case_sensitive=False,
        show_choices=True,
        rich_help_panel="Table Selection",
        # Only allow these two options
        # Typer does not natively restrict to choices, but we can validate
        ),
    output: str = typer.Option('export.csv', help="Output CSV file")
):
    """Export a table to CSV using DuckDB's COPY command."""
    allowed_tables = ['atb_tables', 'nf_stats']
    if table not in allowed_tables:
        allowed_tables_str = ', '.join(allowed_tables)
        typer.echo(f"Error: table must be one of {allowed_tables_str}")
        raise typer.Exit(code=1)
    con = duckdb.connect("nfbac.atb202408.duckdb")
    con.execute(f"COPY {table} TO '{output}' (HEADER, DELIMITER ',')")
    con.close()
    typer.echo(f"Exported {table} to {output}")

@app.command()
def report_taxa_counts(
    min_count: int = typer.Option(1, help="Minimum count to report"),
    output: str = typer.Option('species_counts.tsv', help="Output TSV file"),
    taxon_level: str = typer.Option(default='species', help="Taxonomic rank to count (species or genus)",show_choices=True, case_sensitive=False),
):
    """Write a count of rows for all taxa in all tables that have the specified taxon column, only showing those with at least min_count rows, to a file."""
    duckdb_path = "nfbac.atb202408.duckdb"
    con = duckdb.connect(duckdb_path)
    # Find tables with the selected taxon column
    tables = con.execute(f"SELECT table_name FROM information_schema.columns WHERE column_name = '{taxon_level}'").fetchall()
    with open(output, 'w') as out:
        for (table_name,) in tables:
            result = con.execute(f"SELECT {taxon_level}, COUNT(*) as count FROM {table_name} GROUP BY {taxon_level} HAVING count >= {min_count} ORDER BY count DESC")
            if result.fetchone() is not None:
                out.write(f"# {taxon_level} counts in table '{table_name}' (min_count={min_count}):\n")
                con.execute(f"COPY (SELECT {taxon_level}, COUNT(*) as count FROM {table_name} GROUP BY {taxon_level} HAVING count >= {min_count} ORDER BY count DESC) TO '{output}' (HEADER, DELIMITER '\t')")
    con.close()
    typer.echo(f"{taxon_level} counts written to {output}")

def describe_duckdb(duckdb_path, output_file='duckdb_description.txt'):
    """
    Write a description of the DuckDB database: list all tables, columns, and types.
    """
    try:
        duckdb_conn = duckdb.connect(duckdb_path)
        tables = duckdb_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
        with open(output_file, 'w') as out:
            for (table_name,) in tables:
                out.write(f"Table: {table_name}\n")
                columns = duckdb_conn.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
                if not columns.empty:
                    columns[['name', 'type']].to_csv(out, sep='\t', index=False, header=['column', 'type'])
                out.write("\n")
        duckdb_conn.close()
        logging.info('DuckDB schema description written to %s', output_file)
    except Exception as exc:
        logging.error('Failed to describe DuckDB database: %s', exc)

if __name__ == "__main__":
    app()
