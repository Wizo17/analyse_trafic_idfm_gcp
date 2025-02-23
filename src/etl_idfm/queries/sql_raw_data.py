from etl_idfm.common.config import global_conf

DB_DEFAULT_SCHEMA = global_conf.get("POSTGRES.DB_POSTGRES_DEFAULT_SCHEMA")

STOP_TIMES_DELETE_SQL = f"DROP TABLE IF EXISTS {DB_DEFAULT_SCHEMA}.stop_times;"

STOP_TIMES_CREATE_TABLE_SQL = f"""CREATE TABLE {DB_DEFAULT_SCHEMA}.stop_times (
            trip_id VARCHAR(200),
            arrival_time VARCHAR(200),
            departure_time VARCHAR(200),
            start_pickup_drop_off_window VARCHAR(200),
            end_pickup_drop_off_window VARCHAR(200),
            stop_id VARCHAR(200),
            stop_sequence SMALLINT,
            pickup_type SMALLINT,
            drop_off_type SMALLINT,
            local_zone_id VARCHAR(200),
            stop_headsign TEXT,
            timepoint SMALLINT,
            pickup_booking_rule_id VARCHAR(200),
            drop_off_booking_rule_id VARCHAR(200),
            ingest_date DATE
        );"""

STOP_TIMES_CREATE_INDEX_SQL = f"CREATE INDEX index_stop_times_ingest_date ON {DB_DEFAULT_SCHEMA}.stop_times (ingest_date);"





TRIPS_DELETE_SQL = f"DROP TABLE IF EXISTS {DB_DEFAULT_SCHEMA}.trips;"

TRIPS_CREATE_TABLE_SQL = f"""CREATE TABLE {DB_DEFAULT_SCHEMA}.trips (
            route_id VARCHAR(200),
            service_id VARCHAR(200),
            trip_id VARCHAR(200),
            trip_headsign VARCHAR(200),
            trip_short_name VARCHAR(200),
            direction_id VARCHAR(200),
            block_id VARCHAR(200),
            shape_id VARCHAR(200),
            wheelchair_accessible SMALLINT,
            bikes_allowed SMALLINT,
            ingest_date DATE
        );"""

TRIPS_CREATE_INDEX_SQL = f"CREATE INDEX index_trips_ingest_date ON {DB_DEFAULT_SCHEMA}.trips (ingest_date);"






ROUTES_DELETE_SQL = f"DROP TABLE IF EXISTS {DB_DEFAULT_SCHEMA}.routes;"

ROUTES_CREATE_TABLE_SQL = f"""CREATE TABLE {DB_DEFAULT_SCHEMA}.routes (
            route_id VARCHAR(200),
            agency_id VARCHAR(200),
            route_short_name VARCHAR(200),
            route_long_name VARCHAR(200),
            route_desc TEXT,
            route_type INTEGER,
            route_url VARCHAR(200),
            route_color VARCHAR(200),
            route_text_color VARCHAR(200),
            route_sort_order INTEGER,
            ingest_date DATE
        );"""

ROUTES_CREATE_INDEX_SQL = f"CREATE INDEX index_routes_ingest_date ON {DB_DEFAULT_SCHEMA}.routes (ingest_date);"






STOPS_DELETE_SQL = f"DROP TABLE IF EXISTS {DB_DEFAULT_SCHEMA}.stops;"

STOPS_CREATE_TABLE_SQL = f"""CREATE TABLE {DB_DEFAULT_SCHEMA}.stops (
            stop_id VARCHAR(200),
            stop_code VARCHAR(200),
            stop_name VARCHAR(200),
            stop_desc TEXT,
            stop_lon NUMERIC,
            stop_lat NUMERIC,
            zone_id VARCHAR(200),
            stop_url VARCHAR(200),
            location_type SMALLINT,
            parent_station VARCHAR(200),
            stop_timezone VARCHAR(200),
            level_id VARCHAR(200),
            wheelchair_boarding SMALLINT,
            platform_code TEXT,
            ingest_date DATE
        );"""

STOPS_CREATE_INDEX_SQL = f"CREATE INDEX index_stops_ingest_date ON {DB_DEFAULT_SCHEMA}.stops (ingest_date);"


