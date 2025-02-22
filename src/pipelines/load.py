from google.cloud import bigquery
from pyspark.sql.functions import col
from services.postgres import dataframe_to_table


def load_data_postgres(dataframe, table_name, schema, partition):
    # TODO Add documentation load_data_postgres
    return dataframe_to_table(dataframe, table_name, schema, partition)


def load_data_bigquery(dataframe, gcp_project_id, bigquery_dataset, table_name, partition_colonne):
    # TODO Add documentation load_data_bigquery
    # TODO Put logic into bigquery serice
    dataframe.write.format("bigquery") \
            .option("table", f"{gcp_project_id}.{bigquery_dataset}.{table_name}") \
            .option("partitionField", partition_colonne) \
            .option("partitionOverwriteMode", "dynamic") \
            .mode("overwrite") \
            .save()

