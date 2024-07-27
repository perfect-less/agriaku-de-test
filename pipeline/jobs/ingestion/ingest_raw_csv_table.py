from pipeline.utils.config import Config
from pipeline.utils.whutils import warehouse_to_system_path

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
    target_path = warehouse_to_system_path(
            table_warehouse_path,
            suffix='.parquet'
            )
    df.to_parquet(target_path)

    return target_path
