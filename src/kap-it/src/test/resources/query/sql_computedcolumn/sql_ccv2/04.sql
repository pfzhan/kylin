SELECT round(SUM({fn POWER({fn CONVERT(2, SQL_DOUBLE)}, item_count)}),-290)
FROM test_kylin_fact