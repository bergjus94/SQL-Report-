DROP VIEW IF EXISTS indices CASCADE;
CREATE VIEW indices AS
SELECT
	diff.hobo_id, t_d, t_n, t_nd,
	m.t_avg
FROM diff_mean_daynight diff
JOIN mean_temperature m ON diff.hobo_id=m.hobo_id;
SELECT * FROM indices
