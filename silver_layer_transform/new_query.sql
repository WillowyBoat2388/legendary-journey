WITH test_facility_telemetry_v1 AS (
  select * from `facility-telemetry`
  WHERE entity_type = "facility_sensor_telemetry"
),
test_facility_telemetry AS (
  select * 



  from test_facility_telemetry_v1
),
well_source AS (
  select 
    well_id as WELL_ID, 
    location as WELL_LOCATION,
    cast(`timestamp` as timestamp) AS reading_time, 
    client_id AS drilling_firm, sensor_id as SENSOR, 
    sensor_type as SENSOR_TYPE,
    value as SENSOR_READING_VALUE,
    unit as SENSOR_READING_UNIT
  from `well-telemetry`
  LIMIT 100
),
facility_source AS (
  select entity_type AS aggregate_or_event, client_id as drilling_firm, facility_id, 
  location as facility_location,
  cast(timestamp as timestamp) as `event_time`,
    sensor_id as SENSOR, sensor_type as SENSOR_TYPE, 
    cast(value as int) as SENSOR_READING_VALUE,
    unit as SENSOR_READING_UNIT,
    quality AS `condition`
  from test_facility_telemetry
  limit 100
),
firm_info AS (
  select ws.DRILLING_FIRM, fs.facility_id as FACILITY_ID
  from well_source ws
  INNER JOIN facility_source fs ON ws.drilling_firm=fs.drilling_firm  
),
well_trimmed AS (
  select well_id, drilling_firm, sensor, reading_time 
  from well_source
),
facility_trimmed AS (
  select sensor, facility_location
  from facility_source
),
unit_sensor_monitors AS (
  select DISTINCT sensor_type AS SENSOR_TYPE, facility_location AS DRILLING_SENSOR_SITE
  FROM facility_source fs
  UNION 
  select DISTINCT sensor_type AS SENSOR_TYPE, well_location AS DRILLING_SENSOR_SITE
  FROM well_source ws
),
sensor_monitor AS (
  SELECT DISTINCT sensor_type AS SENSOR_TYPE, drilling_sensor_site AS drilling_sensor_site
  FROM unit_sensor_monitors
),
working as (
  select WELL_ID,
         DRILLING_FIRM,
         SENSOR,
         SENSOR_TYPE,
         WELL_LOCATION,
         READING_TIME,
         SENSOR_READING_VALUE,
         SENSOR_READING_UNIT
  from well_source
),
final as (
  select * from facility_trimmed
)








SELECT * FROM final