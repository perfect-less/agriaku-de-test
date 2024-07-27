from pipeline.utils.whutils import load_warehouse_table


def run_job(
        table_warehouse_path: str,

        table_export_path: str
        ) -> str:
    # Load table
    df = load_warehouse_table(table_warehouse_path)

    # Export table as csv, with ';' separator
    df.to_csv(table_export_path, sep=';', index=False)

    return table_export_path
