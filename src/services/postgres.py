import os
import psycopg2
import psycopg2 
import pandas as pd 
from common.spark_session import SparkSessionInstance
from common.utils import log_message
from sqlalchemy import create_engine 
from common.config import global_conf


DB_HOST = global_conf.get("POSTGRES.DB_HOST")
DB_PORT = global_conf.get("POSTGRES.DB_PORT")
DB_NAME = global_conf.get("POSTGRES.DB_NAME")
DB_USER = global_conf.get("POSTGRES.DB_USER")
DB_PASSWORD = global_conf.get("POSTGRES.DB_PASSWORD")

CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_connection():
    """
    Creates and returns a PostgreSQL connection.

    Return:
    connect object for database
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        #log_message("INFO", "Successful PostgreSQL connection!")
        return conn
    except Exception as e:
        log_message("ERROR", f"Connection error : {e}")
        return None


def execute_query(query, params=None):
    """
    Executes an SQL query.

    Return:
    True if the request has been carried out correctly else False
    """
    conn = get_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                # log_message("INFO", "Request successfully executed!")
            
            return True
        except Exception as e:
            log_message("ERROR", f"Runtime error : {e}")
            return False
        finally:
            conn.close()


def csv_to_table(file_path, table_name, schema_name, partition):
    """
    Loads a csv file into the corresponding table.

    Return:
    True if the data can be integrated else False
    """
    try:
        engine = create_engine(CONN_STRING) 
        part_col = partition[0]
        part_val = partition[1]

        query = f"DELETE FROM {schema_name}.{table_name} WHERE {part_col} = '{part_val}';"

        if execute_query(query):
            # Add data
            df = pd.read_csv(file_path)
            df[part_col] = part_val
            df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema_name)
            # log_message("INFO", f"Adding data to the table {schema_name}.{table_name} successful")
            return True
        
        log_message("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
    except Exception as e:
        log_message("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
    finally:
        engine.dispose()


def dataframe_to_table(dataframe, table_name, schema_name, partition):
    """
    Loads a dataframe into the corresponding table.

    Return:
    True if the data can be integrated else False
    """
    try:
        part_col = partition[0]
        part_val = partition[1]

        query = f"DELETE FROM {schema_name}.{table_name} WHERE {part_col} = '{part_val}';"

        if execute_query(query):            
            log_message("INFO", dataframe.printSchema())

            # Add data
            dataframe.write.jdbc(url=SparkSessionInstance.get_jdbc_url(), table=f"{schema_name}.{table_name}", mode="append", properties=SparkSessionInstance.get_jdbc_properties())
            # log_message("INFO", f"Adding data to the table {schema_name}.{table_name} successful")
            return True
        
        log_message("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
    except Exception as e:
        log_message("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
