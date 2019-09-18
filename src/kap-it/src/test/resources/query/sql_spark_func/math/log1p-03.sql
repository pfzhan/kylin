


SELECT log1p(0),
       log1p(abs(item_count) * abs(cast(price AS bigint)) + 1)
FROM test_kylin_fact
GROUP BY log1p(abs(item_count) * abs(cast(price AS bigint)) + 1),
         item_count,
         price
ORDER BY item_count,
         price LIMIT 10;