from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.whutils import save_warehouse_table


def run_job(
        fact_attendance_weekly_path: str,

        table_warehouse_path: str,
        ) -> str:
    # Load table
    fact_attendance_weekly_df = load_warehouse_table(
            fact_attendance_weekly_path
            )

    # Select relevant columns
    dmart_attendance_summary_weekly_df = fact_attendance_weekly_df.groupby(
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
    dmart_attendance_summary_weekly_df['ATTENDANCE_PCT'] = (
        100 *
        dmart_attendance_summary_weekly_df['ATTENDANCE_SUM'] /
        dmart_attendance_summary_weekly_df['ATTENDANCE_EXPECTED']
        )

    target_path = save_warehouse_table(
            dmart_attendance_summary_weekly_df,
            table_warehouse_path
            )

    return target_path
