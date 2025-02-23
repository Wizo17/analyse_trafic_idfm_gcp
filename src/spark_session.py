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
SPARK_JDBC_PATH = os.getenv("SPARK_JDBC_PATH")
SPARK_HOST = os.getenv("SPARK_HOST")


class SparkSessionSingleton:
    _instance = None
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
                                                .config("spark.driver.host", SPARK_HOST) \
                                                .config("spark.driver.extraClassPath", r"C:\apps\drivers\postgresql-42.7.5.jar") \
                                                .config("spark.executor.extraClassPath", r"C:\apps\drivers\postgresql-42.7.5.jar") \
                                                .config("spark.files.cleanupTime", "0") \
                                                .config("spark.shuffle.service.enabled", "false") \
                                                .master("local[*]") \
                                                .getOrCreate()
        return SparkSessionSingleton._instance
    
    @staticmethod
    def close_instance():
        if SparkSessionSingleton._instance is not None:
            SparkSessionSingleton._instance.stop()
            SparkSessionSingleton._instance = None     

    @staticmethod
    def get_jdbc_url():
        return SparkSessionSingleton._jdbc_url
    
    @staticmethod
    def get_jdbc_properties():
        return SparkSessionSingleton._jdbc_properties

