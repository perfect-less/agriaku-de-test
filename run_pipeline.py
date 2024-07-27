from pipeline.jobs.ingestion import ingest_raw_csv_table
from pipeline.jobs.export import export_table_to_csv

from pipeline.jobs.transformation import dim_enrollment_schedule
from pipeline.jobs.transformation import fact_attendance_daily
from pipeline.jobs.transformation import fact_attendance_weekly
from pipeline.jobs.transformation import dmart_attendance_summary_weekly

from pipeline.utils.config import Config

import os
import logging


logger = logging.getLogger()


def run_pipeline_flows(raw_data_dir: str, export_data_dir: str):
    # INGESTIONS
    r_course_table_path = os.path.join(raw_data_dir, "course.csv")
    r_course_attendance_table_path = os.path.join(raw_data_dir, "course_attendance.csv")
    r_enrollment_table_path = os.path.join(raw_data_dir, "enrollment.csv")
    r_schedule_table_path = os.path.join(raw_data_dir, "schedule.csv")

    ingest_raw_csv_table.run_job(
            r_course_table_path,

            "staging/stg_course",
            )

    ingest_raw_csv_table.run_job(
            r_course_attendance_table_path,

            "staging/stg_course_attendance",
            )

    ingest_raw_csv_table.run_job(
            r_enrollment_table_path,

            "staging/stg_enrollment",
            )

    ingest_raw_csv_table.run_job(
            r_schedule_table_path,

            "staging/stg_schedule",
            )

    # TRANSFORMATIONS
    dim_enrollment_schedule_path = "datamart/dim_enrollment_schedule"
    dim_enrollment_schedule.run_job(
            "staging/stg_course",
            "staging/stg_schedule",
            "staging/stg_enrollment",

            dim_enrollment_schedule_path,
            )

    fact_attendance_daily_path = "datamart/fact_attendance_daily"
    fact_attendance_daily.run_job(
            "staging/stg_course_attendance",
            dim_enrollment_schedule_path,

            fact_attendance_daily_path,
            )

    fact_attendance_weekly_path = "datamart/fact_attendance_weekly"
    fact_attendance_weekly.run_job(
            fact_attendance_daily_path,

            fact_attendance_weekly_path,
            )


    dmart_attendance_summary_weekly_path = "datamart/dmart_attendance_summary_weekly"
    dmart_attendance_summary_weekly.run_job(
            fact_attendance_weekly_path,

            dmart_attendance_summary_weekly_path,
            )

    # EXPORTS
    export_table_to_csv.run_job(
            "datamart/dmart_attendance_summary_weekly",
            os.path.join(export_data_dir, "dmart_attendance_summary_weekly.csv")
            )


def prepare_pipeline():
    logger.info("preparing required directorie(s)")
    # make sure warehouse directory exists.
    if not os.path.isdir(Config.WAREHOUSE_PATH):
        os.makedirs(Config.WAREHOUSE_PATH)
    # make sure export directory exists.
    if not os.path.isdir(Config.EXPORT_DATA_PATH):
        os.makedirs(Config.EXPORT_DATA_PATH)


if __name__ == "__main__":
    logger.info("Pipeline process started.")
    prepare_pipeline()

    logger.info(f"Raw data directory: {Config.RAW_DATA_PATH}")
    logger.info(f"Export data directory: {Config.EXPORT_DATA_PATH}")

    run_pipeline_flows(Config.RAW_DATA_PATH, Config.EXPORT_DATA_PATH)
