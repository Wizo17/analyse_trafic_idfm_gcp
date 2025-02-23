USE base_gtfs;


-- routes
DROP TABLE IF EXISTS trafic_idfm.routes;
CREATE TABLE trafic_idfm.routes
(
    route_id character varying,
    agency_name character varying,
    route_short_name character varying,
    route_long_name character varying,
    route_type character varying,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.routes
    OWNER to postgres;


CREATE INDEX index_routes_ingest_date ON trafic_idfm.routes (ingest_date);



-- trips
DROP TABLE IF EXISTS trafic_idfm.trips;
CREATE TABLE trafic_idfm.trips
(
    trip_id character varying,
    route_id character varying,
    service_id character varying,
    trip_short_name character varying,
    trip_headsign character varying,
    direction_label character varying,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.trips
    OWNER to postgres;


CREATE INDEX index_trips_ingest_date ON trafic_idfm.trips (ingest_date);



-- stops
DROP TABLE IF EXISTS trafic_idfm.stops;
CREATE TABLE trafic_idfm.stops
(
    stop_id character varying,
    stop_name character varying,
    stop_lat decimal,
    stop_lon decimal,
    zone_id character varying,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.stops
    OWNER to postgres;


CREATE INDEX index_stops_ingest_date ON trafic_idfm.stops (ingest_date);



-- stop_times
DROP TABLE IF EXISTS trafic_idfm.stop_times;
CREATE TABLE trafic_idfm.stop_times
(
    trip_id character varying,
    stop_id character varying,
    arrival_time character varying,
    departure_time character varying,
    stop_sequence integer,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.stop_times
    OWNER to postgres;


CREATE INDEX index_stop_times_ingest_date ON trafic_idfm.stop_times (ingest_date);



-- calendar
DROP TABLE IF EXISTS trafic_idfm.calendar;
CREATE TABLE trafic_idfm.calendar
(
    service_id character varying,
    monday smallint,
    tuesday smallint,
    wednesday smallint,
    thursday smallint,
    friday smallint,
    saturday smallint,
    sunday smallint,
    start_date date,
    end_date date,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.calendar
    OWNER to postgres;


CREATE INDEX index_calendar_ingest_date ON trafic_idfm.calendar (ingest_date);




-- transfers
DROP TABLE IF EXISTS trafic_idfm.transfers;
CREATE TABLE trafic_idfm.transfers
(
    from_stop_id character varying,
    to_stop_id character varying,
    transfer_type integer,
    min_transfer_time integer,
    ingest_date date
);

ALTER TABLE IF EXISTS trafic_idfm.transfers
    OWNER to postgres;


CREATE INDEX index_transfers_ingest_date ON trafic_idfm.transfers (ingest_date);


