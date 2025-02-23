import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from common.spark_session import SparkSessionInstance
from pipelines.extract import extract_data
from common.utils import logger
from common.config import global_conf

spark = SparkSessionInstance.get_instance()

def transform_routes(dataframes, partition):
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
    if (global_conf.get("GENERAL.ENV") == "postgres"):
        file_path = os.path.join(f"data/{global_conf.get("GENERAL.DATA_FOLDER")}", file_name)
    else:
        file_path = global_conf.get("GCP.GCP_BUCKET_NAME") + "/" + file_name

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


def transform_trips(dataframes, partition):
    # TODO Add documentation transform_trips
    # TODO Add try catch
    dataframes.createOrReplaceTempView("temp_trips")

    query = f"""
        SELECT 
            trip_id, 
            IF(route_id IS NOT NULL, route_id, '') AS route_id, 
            IF(service_id IS NOT NULL, service_id, '') AS service_id,
            IF(trip_short_name IS NOT NULL, trip_short_name, '') AS trip_short_name,
            IF(trip_headsign IS NOT NULL, trip_headsign, '') AS trip_headsign,
            CASE
                WHEN direction_id = 1 THEN 'ALLER'
                WHEN direction_id = 0 THEN 'RETOUR'
                ELSE ''
            END AS direction_label,
            CAST('{partition[1]}' AS DATE) AS {partition[0]}
        FROM temp_trips 
        WHERE trip_id IS NOT NULL
        """
    df_final = spark.sql(query)

    return df_final


def transform_stops(dataframes, partition):
    # TODO Add documentation transform_stops
    # TODO Add try catch
    df = dataframes.fillna('')
    df.createOrReplaceTempView("temp_stops")

    query = f"""
        SELECT 
            stop_id,
            stop_name,
            CAST(stop_lat AS DECIMAL(9,6)) AS stop_lat,
            CAST(stop_lon AS DECIMAL(9,6)) AS stop_lon,
            zone_id,
            CAST('{partition[1]}' AS DATE) AS {partition[0]}
        FROM temp_stops 
        WHERE stop_id IS NOT NULL AND stop_id != ''
        """
    df_final = spark.sql(query)

    return df_final


def transform_stop_times(dataframes, partition):
    # TODO Add documentation transform_stop_times
    # TODO Add try catch
    df = dataframes.fillna('')
    df.createOrReplaceTempView("temp_stop_times")

    query = f"""
        SELECT 
            trip_id,
            stop_id,
            arrival_time,
            departure_time,
            CAST(stop_sequence AS INTEGER) AS stop_sequence,
            CAST('{partition[1]}' AS DATE) AS {partition[0]}
        FROM temp_stop_times 
        """
    df_final = spark.sql(query)

    return df_final


def transform_calendar(dataframes, partition):
    # TODO Add documentation transform_calendar
    # TODO Add try catch
    df = dataframes.fillna('')
    df.createOrReplaceTempView("temp_calendar")

    query = f"""
        SELECT 
            service_id,
            CAST(monday AS integer) AS monday,
            CAST(tuesday AS integer) AS tuesday,
            CAST(wednesday AS integer) AS wednesday,
            CAST(thursday AS integer) AS thursday,
            CAST(friday AS integer) AS friday,
            CAST(saturday AS integer) AS saturday,
            CAST(sunday AS integer) AS sunday,
            TO_DATE(CAST(start_date AS STRING), 'yyyyMMdd') AS start_date,
            TO_DATE(CAST(end_date AS STRING), 'yyyyMMdd') AS end_date,
            CAST('{partition[1]}' AS DATE) AS {partition[0]}
        FROM temp_calendar 
        """
    df_final = spark.sql(query)

    return df_final


def transform_transfers(dataframes, partition):
    # TODO Add documentation transform_transfers
    # TODO Add try catch
    df = dataframes.fillna('')
    df.createOrReplaceTempView("temp_transfers")

    query = f"""
        SELECT 
            from_stop_id,
            to_stop_id,
            CAST(transfer_type AS integer) AS transfer_type,
            CAST(min_transfer_time AS integer) AS min_transfer_time,
            CAST('{partition[1]}' AS DATE) AS {partition[0]}
        FROM temp_transfers 
        """
    df_final = spark.sql(query)

    return df_final


