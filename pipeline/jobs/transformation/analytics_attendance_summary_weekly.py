from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table
from pipeline.utils.monitor import log_job_call


@log_job_call(name='analytics_attendance_summary_weekly')
def run_job(
        analytics_attendance_weekly_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load table
    analytics_attendance_weekly_df = load_warehouse_table(
            analytics_attendance_weekly_path
            )

    # Select relevant columns
    analytics_attendance_summary_weekly_df = analytics_attendance_weekly_df.groupby(
        by=[
            'SEMESTER_ID',
            'WEEK_ID',
            'COURSE_ID',
            'COURSE_NAME',
        ]
        )[
            ['ATTENDANCE_SUM', 'ATTENDANCE_EXPECTED',]
        ].sum().reset_index()

    # Calculate percentage
    analytics_attendance_summary_weekly_df['ATTENDANCE_PCT'] = (
        100 *
        analytics_attendance_summary_weekly_df['ATTENDANCE_SUM'] /
        analytics_attendance_summary_weekly_df['ATTENDANCE_EXPECTED']
        )
    analytics_attendance_summary_weekly_df = analytics_attendance_summary_weekly_df[[
            'SEMESTER_ID',
            'WEEK_ID',
            'COURSE_NAME',
            'ATTENDANCE_PCT',
        ]]

    target_path = save_warehouse_table(
            analytics_attendance_summary_weekly_df,
            table_warehouse_path
            )

    return target_path
