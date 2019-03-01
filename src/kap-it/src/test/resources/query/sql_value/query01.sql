 SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact t1
	where leaf_categ_id > 80000
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT SUM(price) AS sum_price_2
		FROM test_kylin_fact
		where leaf_categ_id > 80000
		GROUP BY leaf_categ_id
	)
UNION ALL
SELECT cast(1999 as bigint) AS leaf_categ_id, 11.2 AS sum_price, 21.2 AS sum_price2
UNION ALL
SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	where leaf_categ_id > 80000
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	where leaf_categ_id > 80000
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT SUM(price) AS sum_price_2
		FROM test_kylin_fact
		where leaf_categ_id > 80000
		GROUP BY leaf_categ_id
	)
ORDER BY 1