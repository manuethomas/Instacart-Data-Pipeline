import pandas as pd
from utils.configuration import Configuration
from pathlib import Path
from utils.logging import logger

class DataTransformation:
    def __init__(self, config):
        self.source_dir = config.data_transformation.source_dir
        self.output_dir = config.data_transformation.output_dir

    def read_files(self):
        """Takes in raw datasets, applies the tranformations and saves them in csv format
        """
        try:
            self.fact_orders = pd.read_csv(Path(self.source_dir) / "fact_orders.csv")
            self.fact_order_products_prior = pd.read_csv(Path(self.source_dir) / "fact_order_products_prior.csv")
            self.fact_order_products = pd.read_csv(Path(self.source_dir) / "fact_order_products.csv")
            self.dim_products = pd.read_csv(Path(self.source_dir) / "dim_products.csv")
            self.dim_departments = pd.read_csv(Path(self.source_dir) / "dim_departments.csv")
            self.dim_aisles = pd.read_csv(Path(self.source_dir) / "dim_aisles.csv")
        except Exception as e:
            logger.error(f"Failed loading files to dataframe {e}")
            raise e

    def apply_transformation(self):
        """Applies the set transformations to the Dataframes"""

        try:
            self.fact_orders['order_hour_of_day'] = self.fact_orders['order_hour_of_day'].astype('int')
            self.fact_orders['days_since_prior_order'] = self.fact_orders['days_since_prior_order'].astype('Int64')
            self.fact_orders['eval_set'] = self.fact_orders['eval_set'].astype('category')
            self.dim_products['product_name'] = self.dim_products['product_name'].astype('category')
            self.dim_aisles['aisle'] = self.dim_aisles['aisle'].astype('category')
            self.dim_departments['department'] = self.dim_departments['department'].astype('category')

            logger.info("All transformations applied")

        except Exception as e:
            logger.error(f"Applying transformation failed: {e}")
            raise e

    def export_as_csv(self):
        """Export Dataframe to Csv"""
        try:
            self.fact_orders.to_csv(Path(self.output_dir) / 'fact_orders.csv', index=False)
            self.fact_order_products_prior.to_csv(Path(self.output_dir) / 'fact_order_products_prior.csv', index=False)
            self.fact_order_products.to_csv(Path(self.output_dir) / 'fact_order_products.csv', index=False)
            self.dim_products.to_csv(Path(self.output_dir) / 'dim_products.csv', index=False)
            self.dim_departments.to_csv(Path(self.output_dir) / 'dim_departments.csv', index=False)
            self.dim_aisles.to_csv(Path(self.output_dir) / "dim_aisles.csv", index=False)

            logger.info("Converted the transformed files to csv")
        except Exception as e:
            logger.error(f"Failed converting dataframe to csv: {e}")
            raise e 


if __name__ == '__main__':

    # Get config
    config = Configuration.get_config()

    # Transformations
    obj = DataTransformation(config)
    obj.read_files()
    obj.apply_transformation()
    obj.export_as_csv()

    logger.info("----Data Transformation Completed---- \n")