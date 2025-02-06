from utils.common import read_yaml
from constants import CONFIG_FILE_PATH
from pathlib import Path
from utils.logging import logger

class Configuration:
    def __init__(self):
        pass
    
    def get_config(self):
        try:
            self.config = read_yaml(CONFIG_FILE_PATH)   
            logger.info(f"yaml file at {Path.cwd() / CONFIG_FILE_PATH} loaded successfully")
            return self.config
        except Exception as e:
            logger.error(f"Failed loading yaml file at {Path.cwd() / CONFIG_FILE_PATH}")
            raise e
