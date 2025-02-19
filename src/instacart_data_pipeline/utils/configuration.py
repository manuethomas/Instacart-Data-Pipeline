from dotenv import load_dotenv
import os
from instacart_data_pipeline.utils.common import read_yaml
from instacart_data_pipeline.utils.logging import logger

class Configuration:
    def __init__(self):
        pass
    
    @classmethod
    def get_config(self):
        try:
            # load environment variables
            load_dotenv()
            config_path = os.getenv("INSTACART_CONFIG_FILE_PATH")
            self.config = read_yaml(config_path)   
            logger.info(f"yaml file at {config_path} loaded successfully")
            return self.config
        except Exception as e:
            logger.error(f"Failed loading yaml file at {config_path}")
            raise e
