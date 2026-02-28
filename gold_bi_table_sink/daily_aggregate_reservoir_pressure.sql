
CREATE TABLE IF NOT EXISTS avg_daily_pressure AS (
  WITH CTE AS (
    select 
      reservoir_id, 
      date_trunc('day', `timestamp`) as `day`, 
      avg(average_reservoir_pressure_psi) as daily_avg_reservoir_pressure
      avg(injection_rates) as daily_avg_injection_rates
    from base_reservoir
    group by reservoir_id, day
  ), 
  CTE2 AS (
    select 
      reservoir_id,
      median(count(day)) as count_day
    from CTE
  )

  select 
    reservoir_id, 
    day, 
    daily_avg_reservoir_pressure,
    daily_avg_injection_rates
  from CTE C1 JOIN CTE2 C2 ON C1.reservoir_id=C2.reservoir_id
  having count(day) < C2.count_day
  order by 2, 3 desc
);

CREATE TABLE IF NOT EXISTS avg_reservoir_oil_rate AS (
  select 
  client_id, 
  production_date, 
  oil_rate_bpd 
  from raw.`production-daily-data`
);

INSERT INTO avg_reservoir_oil_rate
  SELECT * FROM (
    SELECT client_id, production_date, oil_rate_bpd FROM raw.`production-daily-data`
    HAVING production_date < date_add(current_date(), -1) 
);

INSERT INTO avg_daily_pressure (
  WITH CTE AS (
    select 
      reservoir_id, 
      date_trunc('day', `timestamp`) as `day`, 
      avg(average_reservoir_pressure_psi) as daily_avg_reservoir_pressure,
      avg(injection_rates) as daily_avg_injection_rates
    from base_reservoir
    group by reservoir_id, day
    HAVING `day` < date_add(current_date(), -1) 
  ) 

  select 
    reservoir_id, 
    day, 
    daily_avg_reservoir_pressure,
    daily_avg_injection_rates
  from CTE C1 JOIN CTE2 C2 ON C1.reservoir_id=C2.reservoir_id
  having count(day) < C2.count_day
  order by 2, 3 desc
);