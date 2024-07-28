from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call

import pandas as pd


@log_job_call(name='dim_schedule')
def run_job(
        stg_schedule_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load data
    schedule_df = load_warehouse_table(stg_schedule_path)

    # Convert datetime data to datetime datatype
    schedule_df['START_DT'] = pd.to_datetime(schedule_df['START_DT'], format="%d-%b-%y")
    schedule_df['END_DT'] = pd.to_datetime(schedule_df['END_DT'], format="%d-%b-%y")

    # Join schedule_df with course_df
    dim_schedule_df = schedule_df.rename(
            columns={
                'ID': 'SCHEDULE_ID',
                'START_DT': 'SCHEDULE_START_DT',
                'END_DT': 'SCHEDULE_END_DT',
                'COURSE_DAYS': 'SCHEDULE_COURSE_DAYS',
            }
        )[[
            'SCHEDULE_ID',
            'COURSE_ID',
            'LECTURER_ID',
            'SCHEDULE_START_DT',
            'SCHEDULE_END_DT',
            'SCHEDULE_COURSE_DAYS',
        ]]

    # Save table to warehouse
    target_path = save_warehouse_table(dim_schedule_df, table_warehouse_path)

    return target_path
