from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call


@log_job_call(name='analytics_attendance_weekly')
def run_job(
        fact_attendance_daily_path: str,
        dim_course_path: str,
        dim_schedule_path: str,
        dim_enrollment_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load table
    fact_attendance_daily_df = load_warehouse_table(
            fact_attendance_daily_path
            )
    dim_course_df = load_warehouse_table(dim_course_path)
    dim_schedule_df = load_warehouse_table(dim_schedule_path)
    dim_enrollment_df = load_warehouse_table(dim_enrollment_path)

    # Join fact_attendance_daily with other dims to populate its fields.
    analytics_attendance_weekly_df = fact_attendance_daily_df.merge(
            dim_schedule_df[[
                'SCHEDULE_ID',
                'SCHEDULE_START_DT',
                'SCHEDULE_END_DT',
                'SCHEDULE_COURSE_DAYS',
                ]],
            how='left',
            on='SCHEDULE_ID',
            ).merge(
            dim_enrollment_df[[
                'ENROLLMENT_ID',
                'ACADEMIC_YEAR',
                'ENROLL_DT',
                ]],
            how='left',
            on='ENROLLMENT_ID',
            ).merge(
            dim_course_df,
            how='left',
            on='COURSE_ID',
            )

    # Calculate week ID
    analytics_attendance_weekly_df['WEEK_ID'] = (
        1 +
        analytics_attendance_weekly_df['ATTEND_DT'].dt.isocalendar().week -
        analytics_attendance_weekly_df['SCHEDULE_START_DT'].dt.isocalendar().week
        )

    # Calculate expected attendance
    # currently we assume that there are no holidays throughout the semesters.
    # but in reality this wouldn't be the case and the following calculation
    # would'nt be as simple as this.
    analytics_attendance_weekly_df['ATTENDANCE_EXPECTED'] = \
        analytics_attendance_weekly_df['SCHEDULE_COURSE_DAYS'].apply(
            lambda x: len(x.split(','))
        )

    # Count ATTEND_DT to get total attendance
    analytics_attendance_weekly_df = analytics_attendance_weekly_df.groupby(
        by=[
            'STUDENT_ID',
            'SCHEDULE_ID',
            'ENROLLMENT_ID',
            'WEEK_ID',
            'LECTURER_ID',
            'SEMESTER_ID',
            'COURSE_ID',
            'COURSE_NAME',
            'ACADEMIC_YEAR',
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
            'STUDENT_ID',
            'SCHEDULE_ID',
            'ENROLLMENT_ID',
            'WEEK_ID',
            'LECTURER_ID',
            'SEMESTER_ID',
            'COURSE_ID',
            'COURSE_NAME',
            'ACADEMIC_YEAR',
            'ENROLL_DT',
            'SCHEDULE_START_DT',
            'SCHEDULE_END_DT',
            'SCHEDULE_COURSE_DAYS',
            'ATTENDANCE_SUM',
            'ATTENDANCE_EXPECTED',
        ]]

    # Save table
    target_path = save_warehouse_table(
            analytics_attendance_weekly_df,
            table_warehouse_path
            )

    return target_path
