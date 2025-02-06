from utils.common import logger
from utils.configuration import Configuration
from data.ingest import DataIngestion
from data.transform import DataTransformation
from data.load import DataLoading

def main():
    # Getting config
    config_obj = Configuration()
    config = config_obj.get_config()

    # STAGE: DataIngestion

    logger.info(">>>>>-----DataIngestion Stage-----<<<<<")
    ingest = DataIngestion(config)
    # Download file
    ingest.download_file()
    # Unzip file
    ingest.unzip_file()
    logger.info("----Data Ingestion Completed----\n")

        
    # STAGE: DataTransformation

    logger.info(">>>>>-----DataTransformation Stage-----<<<<<")
    transform = DataTransformation(config)
    transform.read_files()
    transform.apply_transformation()
    transform.export_as_csv()

    logger.info("----Data Transformation Completed---- \n")


    # STAGE: DataLoading

    logger.info(">>>>>-----DataLoading Stage-----<<<<<")
    load = DataLoading(config)
    load.load_data()
    logger.info("----Data Loading Completed---- \n")

if __name__ == '__main__':
    main()