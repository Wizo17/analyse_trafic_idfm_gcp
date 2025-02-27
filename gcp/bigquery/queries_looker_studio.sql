SELECT 
  ingest_date,
  route_type,
  route_short_name,
  COUNT(DISTINCT trip_id) AS trip_count
FROM `analytics-trafic-idfm.analytics_dwh.vm_consolidated`
WHERE 1 = 1
-- AND ingest_date = (SELECT MAX(ingest_date) FROM `analytics-trafic-idfm.analytics_dwh.vm_consolidated`)
-- AND route_type = 'Métro' AND route_short_name = '8'
GROUP BY ingest_date, route_type, route_short_name
ORDER BY trip_count DESC
-- ORDER BY route_type, trip_id, stop_sequence
;