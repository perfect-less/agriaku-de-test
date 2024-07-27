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

    @classmethod
    def print_attributes(cls):
        attrs = vars(cls)
        for k, v in attrs.items():
            if k.startswith('__') or k.endswith('__'):
                continue
            print(f"{k:<20}: {v}")
