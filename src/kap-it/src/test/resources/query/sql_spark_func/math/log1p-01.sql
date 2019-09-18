


SELECT log1p(0),
       log1p(abs(item_count) + 1)
FROM test_kylin_fact
ORDER BY item_count LIMIT 10