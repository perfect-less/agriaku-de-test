from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call

import pandas as pd


@log_job_call(name='dim_enrollment')
def run_job(
        stg_enrollment_path: str,
        dim_course_path: str,
        dim_schedule_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load data
    enrollment_df = load_warehouse_table(stg_enrollment_path)
    dim_course_df = load_warehouse_table(dim_course_path)
    dim_schedule_df = load_warehouse_table(dim_schedule_path)

    # Convert datetime data to datetime datatype
    enrollment_df['ENROLL_DT'] = pd.to_datetime(enrollment_df['ENROLL_DT'], format="%d-%b-%y")

    # Join schedule_df with course_df
    dim_enrollment_df = enrollment_df.rename(
            columns={
                'ID': 'ENROLLMENT_ID',
                'SEMESTER': 'SEMESTER_ID',
            }
        ).merge(
            dim_schedule_df,
            how='left',
            on='SCHEDULE_ID',
        ).merge(
            dim_course_df,
            how='left',
            on='COURSE_ID',
        )[[
            'ENROLLMENT_ID',
            'SCHEDULE_ID',
            'STUDENT_ID',
            'LECTURER_ID',
            'COURSE_ID',
            'ACADEMIC_YEAR',
            'SEMESTER_ID',
            'ENROLL_DT',
        ]]

    # Save table to warehouse
    target_path = save_warehouse_table(dim_enrollment_df, table_warehouse_path)

    return target_path
