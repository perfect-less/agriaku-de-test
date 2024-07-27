from pipeline.utils.whutils import warehouse_to_system_path
from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call

import pandas as pd


@log_job_call(name='dim_enrollment_schedule')
def run_job(
        stg_course_path: str,
        stg_schedule_path: str,
        stg_enrollment_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load data
    course_df = load_warehouse_table(stg_course_path)
    schedule_df = load_warehouse_table(stg_schedule_path)
    enrollment_df = load_warehouse_table(stg_enrollment_path)

    # Convert datetime data to datetime datatype
    schedule_df['START_DT'] = pd.to_datetime(schedule_df['START_DT'], format="%d-%b-%y")
    schedule_df['END_DT'] = pd.to_datetime(schedule_df['END_DT'], format="%d-%b-%y")
    enrollment_df['ENROLL_DT'] = pd.to_datetime(enrollment_df['ENROLL_DT'], format="%d-%b-%y")

    # Join schedule_df with course_df
    schedule_course_df = schedule_df.rename(
            columns={
                'ID': 'SCHEDULE_ID',
                'START_DT': 'SCHEDULE_START_DT',
                'END_DT': 'SCHEDULE_END_DT',
                'COURSE_DAYS': 'SCHEDULE_COURSE_DAYS',
            }
        ).merge(
        course_df.rename(
            columns={
            'ID': 'COURSE_ID',
            'NAME': 'COURSE_NAME',
            }
        ),
        how='left', 
        on='COURSE_ID',
        )

    # Join schedule_course_df with enrollment_df to get final df
    dim_enrollment_schedule_df = enrollment_df.rename(
        columns={
            'ID': 'ENROLLMENT_ID',
            'SEMESTER': 'SEMESTER_ID'
        }
        ).merge(
        schedule_course_df,
        how='left',
        on='SCHEDULE_ID',
        )[[
            'SCHEDULE_ID',
            'ENROLLMENT_ID',
            'STUDENT_ID',
            'LECTURER_ID',
            'COURSE_ID',
            'COURSE_NAME',
            'ACADEMIC_YEAR',
            'SEMESTER_ID',
            'ENROLL_DT',
            'SCHEDULE_START_DT',
            'SCHEDULE_END_DT',
            'SCHEDULE_COURSE_DAYS',
        ]]

    # Save table to warehouse
    target_path = save_warehouse_table(dim_enrollment_schedule_df, table_warehouse_path)

    return target_path
