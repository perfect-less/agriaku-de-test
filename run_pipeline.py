from pipeline.jobs.ingestion import ingest_raw_csv_table
from pipeline.jobs.export import export_table_to_csv

from pipeline.jobs.transformation import dim_course
from pipeline.jobs.transformation import dim_schedule
from pipeline.jobs.transformation import dim_enrollment
from pipeline.jobs.transformation import fact_attendance_daily
from pipeline.jobs.transformation import analytics_attendance_weekly
from pipeline.jobs.transformation import analytics_attendance_summary_weekly

from pipeline.utils.config import Config

import os
import logging


# Setup Logging
logger = logging.getLogger()
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s] [%(filename)-23.23s]  %(message)s")
fileHandler = logging.FileHandler(filename='pipeline_log.log')
fileHandler.setFormatter(logFormatter)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logging.basicConfig(level=logging.INFO, handlers=[fileHandler, consoleHandler])


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
    dim_course_path = "datamart/dim_course"
    dim_course.run_job(
            "staging/stg_course",

            dim_course_path,
            )

    dim_schedule_path = "datamart/dim_schedule"
    dim_schedule.run_job(
            "staging/stg_schedule",

            dim_schedule_path,
            )

    dim_enrollment_path = "datamart/dim_enrollment"
    dim_enrollment.run_job(
            "staging/stg_enrollment",
            dim_course_path,
            dim_schedule_path,

            dim_enrollment_path,
            )

    fact_attendance_daily_path = "datamart/fact_attendance_daily"
    fact_attendance_daily.run_job(
            "staging/stg_course_attendance",
            dim_enrollment_path,

            fact_attendance_daily_path,
            )

    analytics_attendance_weekly_path = "analytics/analytics_attendance_weekly"
    analytics_attendance_weekly.run_job(
            fact_attendance_daily_path,
            dim_course_path,
            dim_schedule_path,
            dim_enrollment_path,

            analytics_attendance_weekly_path,
            )


    analytics_attendance_summary_weekly_path = "analytics/analytics_attendance_summary_weekly"
    analytics_attendance_summary_weekly.run_job(
            analytics_attendance_weekly_path,

            analytics_attendance_summary_weekly_path,
            )

    # EXPORTS
    export_table_to_csv.run_job(
            analytics_attendance_summary_weekly_path,
            os.path.join(export_data_dir, "analytics_attendance_summary_weekly.csv")
            )


def prepare_pipeline():
    logger.info("loading configuration")
    Config.parse('pipeline_conf.yml')
    logger.info("running pipeline with the following config")
    Config.print_attributes(logger)

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
