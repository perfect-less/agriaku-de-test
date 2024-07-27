from pipeline.utils.config import Config

import os


def warehouse_to_system_path(
        warehouse_path: str,
        suffix: str = '',
        ) -> str:
    """
    Convert warehouse path to system path. If directories in warehouse doesn't 
    yet exists, it will also create said directories.
    args:
        1. warehouse_path: e.g, 'staging/stg_course'
        2. (Optional) suffix: e.g, '.parquet'
    returns:
        System path. For example if Config.WAREHOUSE_PATH is set to 'warehouse/'
        then the return value would be
        'warehouse/staging/stg_course.parquet'
    """
    target_path = os.path.join(Config.WAREHOUSE_PATH, warehouse_path+suffix)
    target_dir = os.path.dirname(target_path)
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    return target_path
