import os
from pyspark.sql.utils import AnalysisException
from etl_idfm.common.spark_session import SparkSessionInstance
from etl_idfm.common.utils import log_message
from etl_idfm.common.config import global_conf

def extract_data(file_name, has_header):
    """
    Extract data from a CSV file using Spark.
    
    Args:
        file_name (str): Name of the file to read. Can be a local path or cloud storage path.
        has_header (bool, optional): Whether the CSV file has a header row. Defaults to True.
    
    Returns:
        DataFrame | None: Spark DataFrame containing the file data, or None if extraction fails.
    """
    # Input validation
    if not file_name or not isinstance(file_name, str):
        log_message("ERROR", "File name must be a non-empty string")
        return None
    
    try:
        if global_conf.get("GENERAL.ENV") == "local":
            base_path = f"data/{global_conf.get('GENERAL.DATA_FOLDER')}"
            file_path = os.path.join(base_path, file_name)
            
            # Check if file exists in local environment
            if not os.path.exists(file_path):
                log_message("ERROR", f"File not found: {file_path}")
                return None
        else:
            bucket = global_conf.get("GCP.GCP_BUCKET_NAME")
            file_path = f"{bucket}/{file_name}"

        log_message("INFO", f"Reading file: {file_path}")
        spark = SparkSessionInstance.get_instance()
        df = spark.read.csv(file_path, escape="\"", header=has_header)

        row_count = df.count()
        col_count = len(df.columns)
        log_message("INFO", f"Successfully read {row_count} rows and {col_count} columns from {file_name}")

        return df
    
    except AnalysisException as e:
        log_message("ERROR", f"Spark analysis error while reading {file_name}: {str(e)}")
        return None
    except Exception as e:
        log_message("ERROR", f"Unexpected error while reading {file_name}: {str(e)}")
        return None
    

