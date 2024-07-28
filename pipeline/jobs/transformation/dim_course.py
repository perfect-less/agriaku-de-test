from pipeline.utils.whutils import warehouse_to_system_path
from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call

import pandas as pd


@log_job_call(name='dim_course')
def run_job(
        stg_course_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load data
    course_df = load_warehouse_table(stg_course_path)

    # Rename name to course name to standardize field name in this datamart,
    dim_course_df = course_df.rename(
        columns={
            'ID': 'COURSE_ID',
            'NAME': 'COURSE_NAME'
        }
        )

    # Save table to warehouse
    target_path = save_warehouse_table(dim_course_df, table_warehouse_path)

    return target_path
