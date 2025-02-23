import argparse
import sys
from etl_idfm.processors.etl_processor import ETLProcessor
from etl_idfm.common.utils import log_message

def main():
    parser = argparse.ArgumentParser(description="ETL IDFM GTFS to Postgres/BigQuery DWH")
    parser.add_argument('load_date', type=str, help="Load date")
    args = parser.parse_args()

    log_message("INFO", "*************************** START ETL IDFM GTFS to Postgres/BigQuery DWH ***************************")
    log_message("INFO", f"main.py {args.load_date}")

    processor = ETLProcessor()
    rs = processor.process_all_tables(args.load_date)

    log_message("INFO", "*************************** END ETL IDFM GTFS to Postgres/BigQuery DWH ***************************")

    return rs

if __name__ == "__main__":
    exit_code = main()
    sys.exit(int(not exit_code))