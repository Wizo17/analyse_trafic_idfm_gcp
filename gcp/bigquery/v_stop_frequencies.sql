SELECT
  s.stop_id,
  s.stop_name,
  SUBSTR(st.arrival_time, 1, 2) AS hour_of_day,
  COUNT(DISTINCT st.trip_id) AS trip_count,
  s.ingest_date
FROM `analytics-trafic-idfm.analytics_dwh.stops` s
JOIN `analytics-trafic-idfm.analytics_dwh.stop_times` st ON s.stop_id = st.stop_id AND s.ingest_date = st.ingest_date
GROUP BY s.stop_id, s.stop_name, hour_of_day, s.ingest_date
;