import os
from typing import List, Tuple
from common.spark_session import SparkSessionInstance
from common.config import global_conf
from common.utils import logger
from pipelines.extract import extract_data
from pipelines.transform import (
    transform_calendar, transform_routes, transform_stop_times,
    transform_stops, transform_transfers, transform_trips
)
from pipelines.load import load_data_postgres

class ETLProcessor:
    def __init__(self):
        self.table_infos: List[Tuple[str, str]] = [
            ('routes', 'routes.txt'),
            ('trips', 'trips.txt'),
            ('stops', 'stops.txt'),
            ('stop_times', 'stop_times.txt'),
            ('calendar', 'calendar.txt'),
            ('transfers', 'transfers.txt')
        ]
        # self.table_infos: List[Tuple[str, str]] = [
        #     ('routes', 'routes.txt')
        # ]
        self.transform_functions = {
            'routes': transform_routes,
            'trips': transform_trips,
            'stops': transform_stops,
            'stop_times': transform_stop_times,
            'calendar': transform_calendar,
            'transfers': transform_transfers
        }

    def get_file_path(self, file_name: str) -> str:
        if global_conf.get("GENERAL.ENV") == "postgres":
            return os.path.join(f"data/{global_conf.get('GENERAL.DATA_FOLDER')}", file_name)
        return f"{global_conf.get('GCP.GCP_BUCKET_NAME')}/{file_name}"

    def process_table(self, table_name: str, file_name: str, load_date: str) -> bool:
        """
        Process a single table through the ETL pipeline
        
        Args:
            table_name: Name of the table to process
            file_name: Name of the file to process
            load_date: Loading date
            
        Returns:
            bool: True if processing was successful
        """
        try:
            # Extract
            file_path = self.get_file_path(file_name)
            logger("INFO", f"Extract: {table_name}")
            df = extract_data(file_path, True)

            # Transform
            logger("INFO", f"Transform: {table_name}")
            default_part_name = global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_PART_NAME")
            transform_func = self.transform_functions.get(table_name)
            
            if not transform_func:
                logger("ERROR", f"No transform function found for table: {table_name}")
                return False
                
            df = transform_func(df, (default_part_name, load_date))

            # Load
            # TODO Implement for bigquery
            logger("INFO", f"Load: {table_name}")
            return load_data_postgres(
                df, 
                table_name,
                global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_SCHEMA"),
                (default_part_name, load_date)
            )
            
        except Exception as e:
            logger("ERROR", f"Error processing table {table_name}: {str(e)}")
            return False

    def process_all_tables(self, load_date: str) -> None:
        """
        Process all tables in the ETL pipeline
        
        Args:
            load_date: Loading date
        """
        try:
            for table_name, file_name in self.table_infos:
                success = self.process_table(table_name, file_name, load_date)
                if not success:
                    logger("ERROR", f"Failed to process table: {table_name}")
        finally:
            SparkSessionInstance.close_instance()

