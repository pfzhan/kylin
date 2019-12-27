-- KAP#16104
-- nested window
SELECT
	lag(c_pct_x, 1, 0) OVER (
		PARTITION BY tag ORDER BY sort_dp
		) AS lagx
FROM (
	SELECT tag
				,y + x AS sort_dp
		,
		(
			sum(x) OVER (
				PARTITION BY tag ORDER BY y + x
				)
			) + (sum(x) OVER (PARTITION BY tag)) AS c_pct_x
	FROM (
				SELECT 'model' AS tag
					,item_count AS x
					,price AS y
				FROM TEST_KYLIN_FACT
		) tb1
	)