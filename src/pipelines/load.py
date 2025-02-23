from google.cloud import bigquery
from pyspark.sql.functions import col
from services.postgres import dataframe_to_table


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


def load_data_bigquery(dataframe, gcp_project_id, bigquery_dataset, table_name, partition_colonne):
    # TODO Bigquery - Add documentation load_data_bigquery
    # TODO Bigquery - Put logic into bigquery serice
    dataframe.write.format("bigquery") \
            .option("table", f"{gcp_project_id}.{bigquery_dataset}.{table_name}") \
            .option("partitionField", partition_colonne) \
            .option("partitionOverwriteMode", "dynamic") \
            .mode("overwrite") \
            .save()

