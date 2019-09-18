


SELECT log1p(0),
       log1p(abs(item_count) + 1)
FROM test_kylin_fact
GROUP BY log1p(abs(item_count) + 1),
         item_count
ORDER BY item_count LIMIT 10;