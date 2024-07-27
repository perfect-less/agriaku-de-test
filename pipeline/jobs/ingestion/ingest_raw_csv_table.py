from pipeline.utils.config import Config
from pipeline.utils.whutils import warehouse_to_system_path
from pipeline.utils.whutils import save_warehouse_table

import pandas as pd


def run_job(
        raw_table_path: str,

        table_warehouse_path: str = "",
        ) -> str:
    # Read csv data
    df = pd.read_csv(raw_table_path)

    # Write into warehouse
    # we will write warehouse tables as parquet files.
    # we will overwrite whatever files that have been there before.
    target_path = save_warehouse_table(df, table_warehouse_path)

    return target_path
