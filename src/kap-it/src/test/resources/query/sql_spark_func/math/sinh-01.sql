

SELECT sinh(0),
       sinh(item_count)
FROM test_kylin_fact
ORDER BY item_count LIMIT 10;