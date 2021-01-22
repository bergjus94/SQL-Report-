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

-- joining the calculated correlations values with data from metadata

DROP VIEW IF EXISTS correlation_table CASCADE;
CREATE VIEW correlation_table AS
SELECT
	cor.id, "Tcorr1Y", "Tcorr2Y",
	d.device_id, d.location, d.term_id
FROM corr_table cor
JOIN metadata d ON d.id=cor.id;
SELECT * FROM correlation_table
