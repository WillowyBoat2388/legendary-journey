-- -- CREATE STREAMING TABLE reservoir_strength
CREATE VIEW IF NOT EXISTS RES_INFO AS (
  WITH RESERVOIR_DETAILS AS (
    SELECT DISTINCT split(reservoir_id, '_') AS parts
    FROM raw.reservoir
  ),

  RESERVOIR_SPLIT AS (
    SELECT concat_ws("_", slice(parts, 1, size(parts) - 1)) AS reservoir_name, CAST(parts[size(parts) - 1] AS INT) as reservoir_identity
    FROM RESERVOIR_DETAILS
  )

  select * from reservoir_split
);


CREATE OR REPLACE VIEW RESERVOIR_VIEW AS 
  WITH RESERVOIR_DETAILS AS (
    SELECT reservoir_id, split(reservoir_id, '_') AS parts
    FROM raw.reservoir
  ),

  RESERVOIR_SPLIT AS (
    SELECT concat_ws("_", slice(parts, 1, size(parts) - 1)) AS reservoir_name, CAST(parts[size(parts) - 1] AS INT) as reservoir_identity, reservoir_id as og_res_id
    FROM RESERVOIR_DETAILS
  )
  SELECT client_id, rn.reservoir_name as reservoir_name, rn.reservoir_identity as reservoir_id, avg_reservoir_pressure_psi, pressure_decline_rate_psi_per_day, avg_reservoir_temp_f, active_producing_wells, cumulative_production_oil_mmbbl, cumulative_production_gas_bcf, estimated_remaining_reserves_mmbbl, recovery_factor_pct, water_injection_rate_bpd, gas_injection_rate_mcfd
  from raw.reservoir r JOIN reservoir_split rn ON r.reservoir_id = rn.og_res_id
;


CREATE OR REPLACE VIEW LEASE_VIEW AS (
  SELECT client_id, well_id, well_name, well_status
  FROM base.lease
);

-- CREATE STREAMING VIEW PRODUCING_WELLS AS (
--   SELECT client_id, well_id, well_name, well_status
--   FROM STREAM base.lease
--   WHERE well_status = 'Producing'
-- );

CREATE TABLE IF NOT EXISTS WELL_DETAILS (
  well_id INT,
  well_name string);

MERGE INTO WELL_DETAILS AS WD
USING (
  SELECT well_id, well_name
  FROM base.lease
) AS ls
ON wd.well_id=ls.well_id AND wd.well_name=ls.well_name
WHEN NOT MATCHED 
  THEN INSERT *
;

-- ON WD.firm=ls.well_id, wd.`id`=ls.well_id, wd.well_name=ls.well_name 


