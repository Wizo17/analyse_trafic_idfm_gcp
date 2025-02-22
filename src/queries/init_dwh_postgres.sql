USE base_gtfs;

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

