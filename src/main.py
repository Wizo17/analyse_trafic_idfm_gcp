import os
from dotenv import load_dotenv
import argparse
from pipelines.extract import extract_data
from pipelines.transform import transform_calendar, transform_routes, transform_stop_times, transform_stops, transform_transfers, transform_trips
from pipelines.load import load_data_postgres
from spark_session import SparkSessionSingleton
from utils import is_admin, logger


# TODO Optimize load ENV - use config.py
load_dotenv()

ENV = os.getenv("ENV")
DATA_FOLDER = os.getenv("DATA_FOLDER")
DB_POSTGRES_DEFAULT_SCHEMA = os.getenv("DB_POSTGRES_DEFAULT_SCHEMA")
DB_POSTGRES_DEFAULT_PART_NAME = os.getenv("DB_POSTGRES_DEFAULT_PART_NAME")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")


def main():
    parser = argparse.ArgumentParser(description="ETL IDFM GTFS to BigQuery DWH")
    parser.add_argument('load_date', type=str, help="Load date")
    args = parser.parse_args()

    logger("INFO", f"*************************** START ETL IDFM GTFS to BigQuery DWH ***************************")
    logger("INFO", f"main.py {args.load_date}")

    load_date = args.load_date
    table_infos_list = [
        ('routes', 'routes.txt'),
        ('trips', 'trips.txt'),
        ('stops', 'stops.txt'),
        ('stop_times', 'stop_times.txt'),
        ('calendar', 'calendar.txt'),
        ('transfers', 'transfers.txt')
    ]

    for table_info in table_infos_list:
        process_table(table_info[0], table_info[1], load_date)

    SparkSessionSingleton.close_instance()



def process_table(table_name, file_name, load_date):
    '''
    Create table and/or load data into it

    Parameters:
    table_name (str): Table name
    file_name (str): File to be integrated
    load_date (date): Loading date

    Return:
    Boolean
    '''
    rs = False

    # Extract data
    if (ENV == "postgres"):
        file_path = os.path.join(f"data/{DATA_FOLDER}", file_name)
    else:
        file_path = GCP_BUCKET_NAME + "/" + file_name

    logger("INFO", f"Extract: {table_name}")
    df = extract_data(file_path, True)

    # Transform data
    logger("INFO", f"Transform: {table_name}")
    if (table_name == 'routes'):
        df = transform_routes(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    elif (table_name == 'trips'):
        df = transform_trips(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    elif (table_name == 'stops'):
        df = transform_stops(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    elif (table_name == 'stop_times'):
        df = transform_stop_times(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    elif (table_name == 'calendar'):
        df = transform_calendar(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    elif (table_name == 'transfers'):
        df = transform_transfers(df, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    else:
        return False
    
    # TODO Load depend of ENV
    logger("INFO", f"Load: {table_name}")
    rs = load_data_postgres(df, table_name, DB_POSTGRES_DEFAULT_SCHEMA, (DB_POSTGRES_DEFAULT_PART_NAME, load_date))
    
    return rs


if __name__ == "__main__":
    main()
