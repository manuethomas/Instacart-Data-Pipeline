from dotenv import load_dotenv
import os
from utils.common import read_yaml
from pathlib import Path
from utils.logging import logger

class Configuration:
    def __init__(self):
        pass
    
    @classmethod
    def get_config(self):
        try:
            # load environment variables
            load_dotenv()
            config_path = os.getenv("CONFIG_FILE_PATH")
            self.config = read_yaml(config_path)   
            logger.info(f"yaml file at {Path.cwd() / config_path} loaded successfully")
            return self.config
        except Exception as e:
            logger.error(f"Failed loading yaml file at {Path.cwd() / config_path}")
            raise e
