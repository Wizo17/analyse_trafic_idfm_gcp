import os
import argparse
from common.utils import logger
from queries.sql_raw_data import *
from services.postgres import execute_query
from services.postgres import csv_to_table
from common.config import global_conf

def main():
    parser = argparse.ArgumentParser(description="Add raw data in postgres database")

    parser.add_argument('data_file_folder', type=str, help="Data file folder")
    parser.add_argument('load_date', type=str, help="Load date")
    parser.add_argument('--create_table', type=int, default=0, help="Create table optionnal (default vaule : 0)")

    args = parser.parse_args()

    logger("INFO", f"*************************** START load_raw_data ***************************")
    logger("INFO", f"load_raw_data {args.data_file_folder} {args.load_date} --create_table {args.create_table}")

    table_infos_list = [
        ('stop_times', 'stop_times.txt', 'ingest_date', (STOP_TIMES_DELETE_SQL, STOP_TIMES_CREATE_TABLE_SQL, STOP_TIMES_CREATE_INDEX_SQL)),
        ('trips', 'trips.txt', 'ingest_date', (TRIPS_DELETE_SQL, TRIPS_CREATE_TABLE_SQL, TRIPS_CREATE_INDEX_SQL)),
        ('routes', 'routes.txt', 'ingest_date', (ROUTES_DELETE_SQL, ROUTES_CREATE_TABLE_SQL, ROUTES_CREATE_INDEX_SQL)),
        ('stops', 'stops.txt', 'ingest_date', (STOPS_DELETE_SQL, STOPS_CREATE_TABLE_SQL, STOPS_CREATE_INDEX_SQL))
    ]

    for table_info in table_infos_list:
        process_table(table_info[0], args.data_file_folder, table_info[1], (table_info[2], args.load_date), table_info[3], bool(args.create_table))

    logger("INFO", f"*************************** END load_raw_data ***************************")    




def process_table(table_name, folder, file, part, queries, create_table):
    '''
    Create table and/or load data into it

    Parameters:
    table_name (str): Table name
    folder (str): Subfolder of the data folder containing the data
    file (str): File to be integrated
    part ((str, str)): Key and value for partitioning
    create_table (bool): Creating table or not

    Return:
    Boolean
    '''
    part_col = part[0]
    part_val = part[1]
    if bool(create_table):
        logger("INFO", f"Deleting table: {table_name}")
        rs_delete = execute_query(queries[0])
        logger("INFO", f"Creating table: {table_name}")
        rs_create = execute_query(queries[1])
        logger("INFO", f"Creating index on table: {table_name}")
        rs_index = execute_query(queries[2])
        if rs_delete and rs_create and rs_index:
            logger("INFO", f"Loading data into table: {table_name}")
            return load_data(table_name, folder, file, (part_col, part_val))
    else:
        logger("INFO", f"Loading data into table: {table_name}")
        return load_data(table_name, folder, file, (part_col, part_val))
    
    return False



def load_data(table_name, data_file_folder, file_data, load_part):
    '''
    Create table and/or load data into it

    Parameters:
    table_name (str): Table name
    data_file_folder (str): Subfolder of the data folder containing the data
    file_data ((str, str)): File to be integrated
    load_part (str): Key and value for partitioning

    Return:
    None
    '''
    DB_DEFAULT_SCHEMA = global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_SCHEMA")

    route_file_path = os.path.join(f"data/{data_file_folder}", file_data)
    return csv_to_table(route_file_path, table_name, DB_DEFAULT_SCHEMA, load_part)



main()