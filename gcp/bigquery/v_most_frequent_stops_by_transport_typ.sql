WITH stop_frequency AS (
  SELECT
    r.route_type,
    st.stop_id,
    s.stop_name,
    s.zone_id,
    st.ingest_date,
    COUNT(st.trip_id) AS stop_count
  FROM 
    `analytics-trafic-idfm.analytics_dwh.stop_times` st
  JOIN 
    `analytics-trafic-idfm.analytics_dwh.trips` t ON st.trip_id = t.trip_id AND st.ingest_date = t.ingest_date
  JOIN 
    `analytics-trafic-idfm.analytics_dwh.routes` r ON t.route_id = r.route_id AND t.ingest_date = r.ingest_date
  JOIN 
    `analytics-trafic-idfm.analytics_dwh.stops` s ON st.stop_id = s.stop_id AND st.ingest_date = s.ingest_date
  GROUP BY
    r.route_type,
    st.stop_id,
    s.stop_name,
    s.zone_id,
    st.ingest_date
),
ranked_stops AS (
  SELECT
    route_type,
    stop_id,
    stop_name,
    zone_id,
    ingest_date,
    stop_count,
    ROW_NUMBER() OVER (
      PARTITION BY route_type, ingest_date 
      ORDER BY stop_count DESC
    ) AS rank
  FROM 
    stop_frequency
)
SELECT
  route_type,
  stop_id,
  stop_name,
  zone_id,
  ingest_date,
  stop_count,
  rank
FROM
  ranked_stops
ORDER BY
  route_type,
  ingest_date,
  rank
;