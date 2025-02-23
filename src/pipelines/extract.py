import os
from common.spark_session import SparkSessionInstance
from common.utils import logger


def extract_data(input_path, as_header):
    # TODO Add documentation extract_data
    # TODO Check if it work with cloud storage

    # if global_conf.get("GENERAL.ENV") == "postgres"

    spark = SparkSessionInstance.get_instance()
    
    return spark.read.csv(input_path, escape="\"", header=as_header)

