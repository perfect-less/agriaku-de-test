from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call

import pandas as pd


@log_job_call(name='fact_attendance_daily')
def run_job(
        course_attendance_path: str,
        dim_enrollment_schedule_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load data
    course_attendance_df = load_warehouse_table(course_attendance_path)
    dim_enrollment_schedule_df = load_warehouse_table(dim_enrollment_schedule_path)

    # Convert datetime column to datetime datatype.
    course_attendance_df['ATTEND_DT'] = pd.to_datetime(
            course_attendance_df['ATTEND_DT'],
            format="%d-%b-%y"
            )

    # Join course_attendance_df to dim_enrollment_schedule_df
    fact_attendance_daily_df = course_attendance_df.rename(
        columns={
            'ID': 'ATTENDANCE_ID',
        }).merge(
        dim_enrollment_schedule_df,
        on=['STUDENT_ID', 'SCHEDULE_ID'],
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
            'ATTEND_DT'
        ]]

    # Save table to warehouse
    target_path = save_warehouse_table(fact_attendance_daily_df, table_warehouse_path)

    return target_path
