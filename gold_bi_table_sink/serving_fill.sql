-- Get the max timestamp from serving.well_monitoring to process only new data
CREATE OR REPLACE TEMPORARY VIEW max_ts AS (
    SELECT COALESCE(MAX(timestamp), CAST('1970-01-01' AS TIMESTAMP)) as max_timestamp
    FROM serving.well_monitoring
);

-- Create a temporary view 'mermaid' with cleaned and transformed lease data
-- OPTIMIZATION: Only process records newer than the last processed timestamp
CREATE OR REPLACE TEMPORARY VIEW mermaid AS (
    WITH CTE1 AS (
        -- Filter early to reduce data volume, then split IDs into arrays
        SELECT
            split(client_id, '_') as client_id,
            split(well_id, '_') as well_id,
            split(sensor_id, '_') as sensor_id, 
            timestamp, 
            `flow_bbl/d`, 
            `gas_composition_mol%`, 
            level_ft, 
            pressure_psi, 
            `temperature_degF`, 
            `vibration_mm/s`, 
            location, 
            status, 
            quality
        FROM base.lease
        WHERE timestamp > (SELECT max_timestamp FROM max_ts)  -- INCREMENTAL FILTER
    ),
    CTE2 AS (
        -- Extract numeric IDs and well name, rename columns for clarity
        SELECT 
            CAST(element_at(client_id, size(client_id)) AS INT) as client_id, 
            CAST(element_at(well_id, size(well_id)) AS INT) as well_id,
            concat_ws('_', array_remove(well_id, element_at(well_id, size(well_id)))) as well_name,
            CAST(element_at(sensor_id, size(sensor_id)) AS INT) as sensor_id, 
            timestamp, 
            `flow_bbl/d` as flow_bbl_d, 
            `gas_composition_mol%` as gas_composition_mol_pct, 
            level_ft, 
            pressure_psi, 
            temperature_degF, 
            `vibration_mm/s` as vibration_mm_s, 
            location, 
            status, 
            quality
        FROM cte1
        order by client_id, well_id, sensor_id, timestamp
    ),
    CTE3 AS (
    SELECT DISTINCT * FROM cte2
    )

    select * from CTE3
);

-- Create the well_monitoring table if it doesn't exist, using the mermaid view
CREATE TABLE IF NOT EXISTS serving.well_monitoring AS 
SELECT * FROM MERMAID;

-- Merge new data from mermaid into well_monitoring with schema evolution
-- OPTIMIZATION: Only merges incremental data (filtered by timestamp)
INSERT INTO serving.well_monitoring
    SELECT * 
    FROM mermaid
    ORDER BY `timestamp`, client_id, well_id, sensor_id
;


-- MERGE WITH SCHEMA EVOLUTION INTO serving.well_monitoring AS W
-- USING (
--     SELECT *, cast(`timestamp` as date(timestamp))
--     FROM mermaid
-- ) AS c
-- ON c.client_id=w.client_id AND c.well_id=w.well_id AND c.timestamp=w.timestamp AND c.sensor_id=w.sensor_id
-- WHEN NOT MATCHED THEN
--     INSERT *;
