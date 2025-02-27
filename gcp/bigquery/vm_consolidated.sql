CREATE MATERIALIZED VIEW `analytics-trafic-idfm.analytics_dwh.vm_consolidated` AS
SELECT 
  r.route_id,
  r.agency_name,
  r.route_short_name,
  r.route_type,
  t.trip_id,
  t.trip_headsign,
  st.stop_id,
  st.arrival_time,
  st.departure_time,
  st.stop_sequence,
  sp.stop_name,
  r.ingest_date
FROM `analytics-trafic-idfm.analytics_dwh.routes` r
LEFT JOIN `analytics-trafic-idfm.analytics_dwh.trips` t USING (route_id, ingest_date)
LEFT JOIN `analytics-trafic-idfm.analytics_dwh.calendar` c USING (service_id, ingest_date)
LEFT JOIN `analytics-trafic-idfm.analytics_dwh.stop_times` st USING (trip_id, ingest_date)
LEFT JOIN `analytics-trafic-idfm.analytics_dwh.stops` sp USING (stop_id, ingest_date)
-- WHERE r.ingest_date = '2025-02-25'
-- ORDER BY r.route_type, t.trip_id, st.stop_sequence
;
