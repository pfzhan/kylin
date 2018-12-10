-- example: both table are incremental load, result have two context
--    join
--   /    \
--  A      A (alias B)


SELECT test_kylin_fact.seller_id AS seller_id, fact.cal_dt as cal_dt
FROM test_kylin_fact test_kylin_fact
	JOIN test_kylin_fact fact ON test_kylin_fact.seller_id = fact.seller_id
WHERE fact.cal_dt = '2013-12-02'