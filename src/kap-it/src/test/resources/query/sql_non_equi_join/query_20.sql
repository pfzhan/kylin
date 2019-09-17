-- runtime join of non-equi-join and equi-join
SELECT *
FROM
	(SELECT MAX(test_kylin_fact.price), test_kylin_fact.cal_dt, test_kylin_fact.order_id
	FROM test_kylin_fact
		LEFT JOIN edw.test_cal_dt ON test_kylin_fact.order_id <= '9825'
	GROUP BY test_kylin_fact.cal_dt, test_kylin_fact.order_id
	ORDER BY test_kylin_fact.cal_dt DESC, test_kylin_fact.order_id DESC) T1
	inner join
	(SELECT MAX(test_kylin_fact.price), test_kylin_fact.cal_dt, test_kylin_fact.order_id, test_cal_dt.YEAR_BEG_DT
	FROM test_kylin_fact
		LEFT JOIN edw.test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
	GROUP BY test_kylin_fact.cal_dt, test_kylin_fact.order_id, test_cal_dt.YEAR_BEG_DT ) T2
	on T1.order_id = T2.order_id
	where T1.order_id in (4752, 9825)