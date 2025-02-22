import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv


load_dotenv()

APP_NAME = os.getenv("APP_NAME")
DB_HOST = os.getenv("DB_POSTGRES_HOST")
DB_PORT = os.getenv("DB_POSTGRES_PORT")
DB_NAME = os.getenv("DB_POSTGRES_NAME")
DB_USER = os.getenv("DB_POSTGRES_USER")
DB_PASSWORD = os.getenv("DB_POSTGRES_PASSWORD")
PATH_POSTGRES_CONNECTOR_JAR = os.getenv("PATH_POSTGRES_CONNECTOR_JAR")


class SparkSessionSingleton:
    _instance = None
    _instance_postgres = None
    _jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    _jdbc_properties = {
        "user": f"{DB_USER}",
        "password": f"{DB_PASSWORD}",
        "driver": "org.postgresql.Driver"
    }

    @staticmethod
    def get_instance():
        if SparkSessionSingleton._instance is None:
            SparkSessionSingleton._instance = SparkSession.builder \
                .appName(f"GLOBAL: {APP_NAME}") \
                .getOrCreate()
        return SparkSessionSingleton._instance
    
    @staticmethod
    def close_instance():
        if SparkSessionSingleton._instance is not None:
            SparkSessionSingleton._instance.stop()
            SparkSessionSingleton._instance = None

    @staticmethod
    def get_instance_postgres():
        if SparkSessionSingleton._instance_postgres is None:
            SparkSessionSingleton._instance_postgres = SparkSession.builder \
                .appName(f"POSTGRES: {APP_NAME}") \
                .config("spark.driver.host", DB_HOST) \
                .config("spark.jars", PATH_POSTGRES_CONNECTOR_JAR) \
                .getOrCreate()
        return SparkSessionSingleton._instance_postgres
            
    @staticmethod
    def close_instance():
        if SparkSessionSingleton._instance_postgres is not None:
            SparkSessionSingleton._instance_postgres.stop()
            SparkSessionSingleton._instance_postgres = None        

    @staticmethod
    def get_jdbc_url():
        return SparkSessionSingleton._jdbc_url
    
    @staticmethod
    def get_jdbc_properties():
        return SparkSessionSingleton._jdbc_properties

