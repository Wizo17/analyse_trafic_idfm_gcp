from google.cloud import bigquery
from pyspark.sql.functions import col
from etl_idfm.services.bigquery import write_to_partitioned_bq
from etl_idfm.services.postgres import dataframe_to_table



def load_data_postgres(dataframe, table_name, schema, partition):
    """
    Extract data from a CSV file using Spark.
    
    Args:
        dataframe (DataFrame Spark): Name of the file to read. Can be a local path or cloud storage path.
        table_name (str): Table name.
        schema (str): Database schema.
        partition ((str, str)): Column and value for partition.
    
    Returns:
        DataFrame | None: Spark DataFrame containing the file data, or None if transformation fails.
    """
    return dataframe_to_table(dataframe, table_name, schema, partition)


def load_data_bigquery(dataframe, gcp_project_id, bigquery_dataset, table_name, partition):
    """
    Load Dataframe pyspark to bigquery table
    
    Args:
        dataframe (DataFrame PySaprk): The DataFrame to write
        gcp_project_id (str): GCP project ID
        bigquery_dataset (str): BigQuery dataset ID
        table_name (str): BigQuery table ID
        partition (str): Name and Value of the partition
        
    Returns:
        bool: True if write successful, False otherwise
    """
    return write_to_partitioned_bq(dataframe, gcp_project_id, bigquery_dataset, table_name, partition)

