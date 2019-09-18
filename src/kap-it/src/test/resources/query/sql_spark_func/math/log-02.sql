


SELECT log(10, 100),
       log(10, item_count + 1)
FROM test_kylin_fact
ORDER BY item_count LIMIT 2;