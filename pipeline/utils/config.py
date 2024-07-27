import os
import yaml
import logging


logger = logging.getLogger(__name__)


class Config(object):
    # Singleton pattern
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Config, cls).__new__(cls)
        return cls.instance

    # Default value
    WAREHOUSE_PATH = "warehouse/"
    RAW_DATA_PATH = "raw-data/"
    EXPORT_DATA_PATH = "export-data/"

    # Parse pipeline_conf.yml
    @classmethod
    def parse(cls, config_file_path: str = './pipeline_conf.yml'):
        if not os.path.isfile(config_file_path):
            logger.warning(
                    f"config file {config_file_path} doesn't exist.\n"
                    "Defaulting to default configs."
                    )

        with open(config_file_path, 'r') as yml_file:
            yml_conf: dict = yaml.safe_load(yml_file)
        yml_file.close()

        cls.WAREHOUSE_PATH = yml_conf.get('warehouse_path', cls.WAREHOUSE_PATH)
        cls.RAW_DATA_PATH = yml_conf.get('raw_data_path', cls.RAW_DATA_PATH)
        cls.EXPORT_DATA_PATH = yml_conf.get('export_data_path', cls.EXPORT_DATA_PATH)

    @classmethod
    def print_attributes(cls, logger):
        attrs = vars(cls)
        for k, v in attrs.items():
            if k.startswith('__') or k.endswith('__'):
                continue
            logger.info(f"{k:<20}: {v}")
