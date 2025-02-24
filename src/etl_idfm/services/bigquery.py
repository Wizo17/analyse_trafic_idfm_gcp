from google.cloud import bigquery
from etl_idfm.common.utils import log_message


def write_to_partitioned_bq(dataframe, gcp_project_id, bigquery_dataset, table_name, partition, write_mode="overwrite"):
    """
    Write a PySpark DataFrame to a partitioned BigQuery table.
    If data exists for the specified partition, it will be deleted before writing.
    
    Args:
        dataframe (DataFrame PySaprk): The DataFrame to write
        gcp_project_id (str): GCP project ID
        bigquery_dataset (str): BigQuery dataset ID
        table_name (str): BigQuery table ID
        partition (str): Name and Value of the partition
        write_mode (str): Write mode ('overwrite' or 'append')
        
    Returns:
        bool: True if write successful, False otherwise
    """
    
    # Construct full table path
    table_path = f"{gcp_project_id}.{bigquery_dataset}.{table_name}"
    
    try:
        # Initialize BigQuery client
        bq_client = bigquery.Client(project=gcp_project_id)

        log_message("INFO", dataframe.printSchema())
        
        # Check if table exists
        try:
            table = bq_client.get_table(table_path)
            table_exists = True
        except Exception:
            table_exists = False
            
        # If table exists and we're overwriting, delete the partition
        if table_exists and write_mode == "overwrite":
            query = f"""
            DELETE FROM `{table_path}`
            WHERE DATE({partition[0]}) = '{partition[1]}'
            """
            query_job = bq_client.query(query)
            query_job.result()  # Wait for query to complete
            
        # Configure write options
        write_options = {
            "table": table_path,
            "partitionField": partition[0],
            "partitionType": "DAY",
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND",
            "writeMethod": "direct"
        }
        
        # Write to BigQuery
        dataframe.write.format("bigquery") \
            .options(**write_options) \
            .mode("append") \
            .save()
            
        return True
        
    except Exception as e:
        print(f"Error writing to BigQuery: {str(e)}")
        return False
    
