import argparse
from processors.etl_processor import ETLProcessor
from common.utils import logger

def main():
    parser = argparse.ArgumentParser(description="ETL IDFM GTFS to BigQuery DWH")
    parser.add_argument('load_date', type=str, help="Load date")
    args = parser.parse_args()

    logger("INFO", "*************************** START ETL IDFM GTFS to BigQuery DWH ***************************")
    logger("INFO", f"main.py {args.load_date}")

    processor = ETLProcessor()
    processor.process_all_tables(args.load_date)

if __name__ == "__main__":
    main()