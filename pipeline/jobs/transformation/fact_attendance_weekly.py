from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call


@log_job_call(name='fact_attendance_weekly')
def run_job(
        fact_attendance_daily_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load table
    fact_attendance_daily_df = load_warehouse_table(
            fact_attendance_daily_path
            )

    # Calculate week ID
    fact_attendance_weekly_df = fact_attendance_daily_df
    fact_attendance_weekly_df['WEEK_ID'] = (
        1 +
        fact_attendance_daily_df['ATTEND_DT'].dt.isocalendar().week -
        fact_attendance_daily_df['SCHEDULE_START_DT'].dt.isocalendar().week
        )

    # Calculate expected attendance
    # currently we assume that there are no holidays throughout the semesters.
    # but in reality this wouldn't be the case and the following calculation
    # would'nt be as simple as this.
    fact_attendance_weekly_df['ATTENDANCE_EXPECTED'] = \
        fact_attendance_daily_df['SCHEDULE_COURSE_DAYS'].apply(
            lambda x: len(x.split(','))
        )

    # Count ATTEND_DT to get total attendance
    fact_attendance_weekly_df = fact_attendance_weekly_df.groupby(
        by=[
            'SCHEDULE_ID',
            'ENROLLMENT_ID',
            'STUDENT_ID',
            'LECTURER_ID',
            'COURSE_ID',
            'COURSE_NAME',
            'ACADEMIC_YEAR',
            'SEMESTER_ID',
            'WEEK_ID',
            'ENROLL_DT',
            'SCHEDULE_START_DT',
            'SCHEDULE_END_DT',
            'SCHEDULE_COURSE_DAYS',
            'ATTENDANCE_EXPECTED',
        ]
        )['ATTEND_DT'].count().reset_index() \
        .rename(
            columns={
                'ATTEND_DT': 'ATTENDANCE_SUM'
            }
        )[[
            'SCHEDULE_ID',
            'ENROLLMENT_ID',
            'STUDENT_ID',
            'LECTURER_ID',
            'COURSE_ID',
            'COURSE_NAME',
            'ACADEMIC_YEAR',
            'SEMESTER_ID',
            'WEEK_ID',
            'ENROLL_DT',
            'SCHEDULE_START_DT',
            'SCHEDULE_END_DT',
            'SCHEDULE_COURSE_DAYS',
            'ATTENDANCE_SUM',
            'ATTENDANCE_EXPECTED',
        ]]

    # Save table
    target_path = save_warehouse_table(
            fact_attendance_weekly_df,
            table_warehouse_path
            )

    return target_path
