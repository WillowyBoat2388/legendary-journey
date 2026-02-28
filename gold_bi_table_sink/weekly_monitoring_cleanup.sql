
-- Drop Old data since archive records remain 
DELETE FROM serving.well_monitoring
WHERE timestamp < (SELECT MAX(timestamp) - INTERVAL '7 days' FROM serving.well_monitoring);


-- Add table properties for better performance
ALTER TABLE serving.well_monitoring SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');