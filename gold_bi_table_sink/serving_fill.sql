-- Create a temporary view 'mermaid' with cleaned and transformed lease data
CREATE OR REPLACE TEMPORARY VIEW mermaid AS (
    WITH CTE1 AS (
        -- Split IDs into arrays for further processing
        select
            split(client_id, '_') as client_id,
            split(well_id, '_') as well_id,
            split(sensor_id, '_') as sensor_id, timestamp, `flow_bbl/d`, `gas_composition_mol%`, level_ft, pressure_psi, `temperature_degF`, `vibration_mm/s`, location, status, quality
        from base.lease
    ),
    CTE2 AS (
        -- Extract numeric IDs and well name, rename columns for clarity
        select 
            cast(
                element_at(client_id, 
                size(client_id)) as int
                ) as client_id, 
            cast(
                element_at(well_id, 
                size(well_id)) as int
                ) as well_id,
            concat_ws('_', array_remove(well_id, element_at(well_id, size(well_id)))) as well_name,
            cast(element_at(sensor_id, size(sensor_id)) as int) as sensor_id, timestamp, 
            `flow_bbl/d` as flow_bbl_d, `gas_composition_mol%` as gas_composition_mol_pct, 
            level_ft, pressure_psi, temperature_degF, 
            `vibration_mm/s` as vibration_mm_s, location, status, quality
        from cte1
        order by client_id, well_id, sensor_id, timestamp
    ),
    CTE3 AS (
        -- Select only unique client_id, well_id, timestamp and sensor_id
        select distinct client_id, well_id, well_name, timestamp, sensor_id, flow_bbl_d, gas_composition_mol_pct, level_ft, pressure_psi, temperature_degF, vibration_mm_s, location, status, quality
        from cte2
    )
    
select *
from cte3);

-- Create the well_monitoring table if it doesn't exist, using the mermaid view
CREATE TABLE IF NOT EXISTS serving.well_monitoring AS 
SELECT * FROM MERMAID;

-- Merge new data from mermaid into well_monitoring with schema evolution
MERGE WITH SCHEMA EVOLUTION INTO serving.well_monitoring AS w
USING (
    select *
    from mermaid
) AS c
ON c.client_id=w.client_id and c.well_id=w.well_id and c.timestamp=w.timestamp and c.sensor_id=w.sensor_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *