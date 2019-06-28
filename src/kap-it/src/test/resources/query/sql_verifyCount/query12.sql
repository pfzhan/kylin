SELECT hour(CAL_DT) AS "hour", minute(CAL_DT) AS "minute"
	, second(CAL_DT) AS "second"
FROM test_kylin_fact
GROUP BY CAL_DT
LIMIT 1