

SELECT price,
       conv(cast(price * cbrt(item_count) AS bigint), 10, 16)
FROM test_kylin_fact
ORDER BY price,
         item_count LIMIT 10;