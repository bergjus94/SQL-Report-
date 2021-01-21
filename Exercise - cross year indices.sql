-- calculating the correlation from this years time series to the time series of 
-- the clostest HOBO from 2020 and 2019

--Creating View from metadata with columns for the meta_id of the clostest stations in year 2019 an 2020

DROP VIEW IF EXISTS meta_distance CASCADE;
CREATE VIEW meta_distance AS
WITH meta_distance AS (
	SELECT *, 
	(SELECT id FROM metadata ly WHERE term_id=9 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta20_id,
	(SELECT id FROM metadata ly WHERE term_id=7 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta19_id
	FROM metadata m
	WHERE term_id=11 AND sensor_id=1
	)
SELECT * 
FROM meta_distance;


-- ordering the time series from 1 to ... because dates of different time series are not the same, 
-- but we still want to correlate all values

DROP VIEW IF EXISTS data_norm;
CREATE VIEW data_norm AS
SELECT
	row_number() OVER (PARTITION BY meta_id, variable_id ORDER BY tstamp ASC) as measurement_index,
	*,
	value - avg(value) OVER (PARTITION BY meta_id, variable_id) AS norm,
	avg(value) OVER (PARTITION BY meta_id, variable_id) AS group_avg	
FROM data;
SELECT * FROM data_norm;

-- calculating the correlation with the clostest HOBO device from year 2020 and year 2019

DROP VIEW IF EXISTS corr_table CASCADE;
CREATE VIEW corr_table AS  
SELECT 
	meta_distance.id, 								
	avg(d.value) AS "mean",					
	corr(d.norm, d20.norm) AS "Tcorr1Y",
	corr(d.norm, d19.norm) AS "Tcorr2Y"
FROM data_norm d														
JOIN meta_distance on meta_distance.id = d.meta_id		
JOIN metadata m20 on meta_distance.close_meta20_id=m20.id 
JOIN metadata m19 on meta_distance.close_meta19_id=m19.id
JOIN data_norm d20 on m20.id=d20.meta_id 
	AND d.measurement_index=d20.measurement_index
JOIN data_norm d19 on m19.id=d19.meta_id 
	AND d.measurement_index=d19.measurement_index
GROUP BY meta_distance.id;
SELECT * FROM corr_table;

-- joining the correlation VIEW with meatdata

DROP TABLE IF EXISTS correlation_table CASCADE;
CREATE TABLE correlation_table AS
SELECT
	cor.id, "Tcorr1Y", "Tcorr2Y",
	d.device_id, d.location, d.term_id
FROM corr_table cor
JOIN metadata d ON d.id=cor.id;
SELECT * FROM correlation_table

