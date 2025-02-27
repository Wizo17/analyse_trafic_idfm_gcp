SELECT 
  s.stop_id,
  s.stop_name,
  ST_GEOGPOINT(s.stop_lon, s.stop_lat) AS emplacement,
  JSON_OBJECT(
    'type', 'Point',
    'coordinates', JSON_ARRAY(stop_lon, stop_lat)
  ) AS geojson,
  s.stop_lat,
  s.stop_lon,
  s.zone_id,
  s.ingest_date
FROM `analytics-trafic-idfm.analytics_dwh.stops` s
WHERE 1 = 1
  AND s.stop_lat IS NOT NULL
  AND s.stop_lon IS NOT NULL
;
