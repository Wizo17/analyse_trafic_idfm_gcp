import argparse
from processors.etl_processor import ETLProcessor
from common.utils import log_message

def main():
    parser = argparse.ArgumentParser(description="ETL IDFM GTFS to Postgres/BigQuery DWH")
    parser.add_argument('load_date', type=str, help="Load date")
    args = parser.parse_args()

    log_message("INFO", "*************************** START ETL IDFM GTFS to Postgres/BigQuery DWH ***************************")
    log_message("INFO", f"main.py {args.load_date}")

    processor = ETLProcessor()
    processor.process_all_tables(args.load_date)

    log_message("INFO", "*************************** END ETL IDFM GTFS to Postgres/BigQuery DWH ***************************")

if __name__ == "__main__":
    main()