from pipeline.utils.config import Config

import pandas as pd
import os


def load_warehouse_table(
        warehouse_path: str,
        ) -> pd.DataFrame:
    """
    Load warehouse table as pandas DataFrame.
    args:
        1. warehouse_path: e.g, 'staging/stg_course'
    returns:
        pandas DataFrame
    """
    df = pd.read_parquet(
            warehouse_to_system_path(
                warehouse_path,
                '.parquet'
                )
            )
    return df


def save_warehouse_table(
        df: pd.DataFrame,
        warehouse_path: str,
        ) -> str:
    """
    Save pandas DataFrame as warehouse table.
    args:
        1. df: Table to save as pandas DataFrame
        2. warehouse_path: e.g, 'staging/stg_course'
    returns:
        System path of saved table. For example if Config.WAREHOUSE_PATH is set to 'warehouse/'
        then the return value would be
        'warehouse/staging/stg_course.parquet'
    """
    target_path = warehouse_to_system_path(warehouse_path, '.parquet')
    df.to_parquet(target_path)
    return target_path


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
