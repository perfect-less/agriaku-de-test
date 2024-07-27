from pipeline.utils.whutils import load_warehouse_table
from pipeline.utils.monitor import log_job_call

import logging
import os

logger = logging.getLogger(__name__)


@log_job_call(name='export-csv')
def run_job(
        table_warehouse_path: str,

        table_export_path: str
        ) -> str:
    logger.info(f"Exporting {os.path.basename(table_warehouse_path)} from warehouse.")
    # Load table
    df = load_warehouse_table(table_warehouse_path)

    # Export table as csv, with ';' separator
    df.to_csv(table_export_path, sep=';', index=False)

    return table_export_path
