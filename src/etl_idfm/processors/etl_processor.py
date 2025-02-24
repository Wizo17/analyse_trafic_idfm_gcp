import os
from typing import List, Tuple
from etl_idfm.common.spark_session import SparkSessionInstance
from etl_idfm.common.config import global_conf
from etl_idfm.common.utils import log_message
from etl_idfm.pipelines.extract import extract_data
from etl_idfm.pipelines.transform import (
    transform_calendar, transform_routes, transform_stop_times,
    transform_stops, transform_transfers, transform_trips
)
from etl_idfm.pipelines.load import load_data_bigquery, load_data_postgres

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
            log_message("INFO", f"Extract: {table_name}")
            df = extract_data(file_name, True)
            if df is None: 
                log_message("ERROR", f"A problem occurred during data extraction: {table_name}")
                return False

            # Transform
            log_message("INFO", f"Transform: {table_name}")
            default_part_name = global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_PART_NAME")
            transform_func = self.transform_functions.get(table_name)
            
            if not transform_func:
                log_message("ERROR", f"No transform function found for table: {table_name}")
                return False
                
            df = transform_func(df, (default_part_name, load_date))
            if df is None: 
                log_message("ERROR", f"A problem occurred during data transformation: {table_name}")
                return False

            # Load
            log_message("INFO", f"Load: {table_name}")
            if global_conf.get("GENERAL.ENV") == "local":
                return load_data_postgres(
                    df, 
                    table_name,
                    global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_SCHEMA"),
                    (default_part_name, load_date)
                )
            else:
                return load_data_bigquery(
                    df, 
                    global_conf.get("GCP.GCP_PROJECT_ID"),
                    global_conf.get("GCP.GCP_BIGQUERY_DATASET"),
                    table_name,
                    (default_part_name, load_date)
                )
                
        except Exception as e:
            log_message("ERROR", f"Error processing table {table_name}: {str(e)}")
            return False

    def process_all_tables(self, load_date: str) -> bool:
        """
        Process all tables in the ETL pipeline
        
        Args:
            load_date: Loading date
        """
        try:
            all_process_success = True
            for table_name, file_name in self.table_infos:
                success = self.process_table(table_name, file_name, load_date)
                if success:
                    log_message("INFO", f"Process table successful: {table_name}")
                else:
                    all_process_success = False
                    log_message("ERROR", f"Failed to process table: {table_name}")
            
            return all_process_success
        except Exception as e:
            return False
        finally:
            SparkSessionInstance.close_instance()

