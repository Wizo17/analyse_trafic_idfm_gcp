


-- routes
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.routes`;
CREATE TABLE `analytics-trafic-idfm.analytics_dwh.routes`
(
    route_id STRING,
    agency_name STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type STRING,
    ingest_date DATE
)
PARTITION BY ingest_date;




-- trips
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.trips`;
CREATE OR REPLACE TABLE `analytics-trafic-idfm.analytics_dwh.trips`
(
    route_id STRING,
    agency_name STRING,
    route_short_name STRING,
    route_long_name STRING,
    route_type STRING,
    ingest_date DATE
)
PARTITION BY ingest_date;



-- stops
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.stops`;
CREATE OR REPLACE TABLE `analytics-trafic-idfm.analytics_dwh.stops`
(
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT64,
    stop_lon FLOAT64,
    zone_id STRING,
    ingest_date DATE
)
PARTITION BY ingest_date;




-- stop_times
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.stop_times`;
CREATE OR REPLACE TABLE `analytics-trafic-idfm.analytics_dwh.stop_times`
(
    trip_id STRING,
    stop_id STRING,
    arrival_time STRING,
    departure_time STRING,
    stop_sequence INT64,
    ingest_date DATE
)
PARTITION BY ingest_date;



-- calendar
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.calendar`;
CREATE OR REPLACE TABLE `analytics-trafic-idfm.analytics_dwh.calendar`
(
    service_id STRING,
    monday INT64,
    tuesday INT64,
    wednesday INT64,
    thursday INT64,
    friday INT64,
    saturday INT64,
    sunday INT64,
    start_date DATE,
    end_date DATE,
    ingest_date DATE
)
PARTITION BY ingest_date;



-- transfers
DROP TABLE IF EXISTS `analytics-trafic-idfm.analytics_dwh.transfers`;
CREATE OR REPLACE TABLE `analytics-trafic-idfm.analytics_dwh.transfers`
(
    from_stop_id STRING,
    to_stop_id STRING,
    transfer_type INT64,
    min_transfer_time INT64,
    ingest_date DATE
)
PARTITION BY ingest_date;


