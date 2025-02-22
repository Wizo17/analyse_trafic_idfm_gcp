import os
from dotenv import load_dotenv
from pyspark.sql.functions import *
from pyspark.sql.types import *
from .extract import extract_data
from utils import logger

load_dotenv()

ENV = os.getenv("ENV")
DATA_FOLDER = os.getenv("DATA_FOLDER")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

def transform_route(dataframes, partition):
    # TODO Add documentation transform_route
    # TODO Add try catch
    df_routes = dataframes.filter((col("route_id").isNotNull()) & (col("route_id") != ""))

    df_routes = df_routes.withColumn("route_type", 
                   when(col("route_type") == 0, "Tramway")
                   .when(col("route_type") == 1, "Métro")
                   .when(col("route_type") == 2, "Train")
                   .when(col("route_type") == 3, "Bus")
                   .when(col("route_type") == 7, "Funiculaire")
                   .otherwise(""))
    
    file_name = "agency.txt"
    if (ENV == "postgres"):
        file_path = os.path.join(f"data/{DATA_FOLDER}", file_name)
    else:
        file_path = GCP_BUCKET_NAME + "/" + file_name

    df_agency = extract_data(file_path, True)
    df_joined = df_routes.join(df_agency, on="agency_id", how="left")

    df_selected = df_joined.select('route_id', 'agency_name', 'route_short_name', 'route_long_name', 'route_type')
    df_selected = df_selected.fillna({"agency_name": "", "route_short_name": "", "route_long_name": ""})

    df_final = df_selected.withColumn("route_id", col("route_id").cast(StringType())) \
              .withColumn("agency_name", col("agency_name").cast(StringType())) \
              .withColumn("route_short_name", col("route_short_name").cast(StringType())) \
              .withColumn("route_long_name", col("route_long_name").cast(StringType())) \
              .withColumn("route_type", col("route_type").cast(StringType())) \
              .withColumn(partition[0], to_date(lit(partition[1]), "yyyy-MM-dd"))

    return df_final


