import os
from dotenv import load_dotenv
from spark_session import SparkSessionSingleton
from utils import logger

load_dotenv()

ENV = os.getenv("ENV")

def extract_data(input_path, as_header):
    # TODO Add documentation extract_data
    # TODO Check if it work with cloud storage

    spark = SparkSessionSingleton.get_instance()
    
    return spark.read.csv(input_path, escape="\"", header=as_header)

