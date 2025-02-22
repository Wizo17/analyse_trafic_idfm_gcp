import os
import psycopg2
import psycopg2 
import pandas as pd 
from spark_session import SparkSessionSingleton
from utils import logger
from sqlalchemy import create_engine 
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_POSTGRES_HOST")
DB_PORT = os.getenv("DB_POSTGRES_PORT")
DB_NAME = os.getenv("DB_POSTGRES_NAME")
DB_USER = os.getenv("DB_POSTGRES_USER")
DB_PASSWORD = os.getenv("DB_POSTGRES_PASSWORD")

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
        #logger("INFO", "Successful PostgreSQL connection!")
        return conn
    except Exception as e:
        logger("ERROR", f"Connection error : {e}")
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
                # logger("INFO", "Request successfully executed!")
            
            return True
        except Exception as e:
            logger("ERROR", f"Runtime error : {e}")
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
            # logger("INFO", f"Adding data to the table {schema_name}.{table_name} successful")
            return True
        
        logger("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
    except Exception as e:
        logger("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
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
            # Add data
            dataframe.write.jdbc(url=SparkSessionSingleton.get_jdbc_url(), table=f"{schema_name}.{table_name}", mode="append", properties=SparkSessionSingleton.get_jdbc_properties())
            # logger("INFO", f"Adding data to the table {schema_name}.{table_name} successful")
            return True
        
        logger("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
    except Exception as e:
        logger("ERROR", f"Runtime error - Adding data to the table {schema_name}.{table_name} : {e}")
        return False
