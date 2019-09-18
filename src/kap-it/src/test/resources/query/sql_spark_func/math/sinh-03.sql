


SELECT (sinh(price) - sinh(item_count)) / (sinh(cast(price AS bigint) + item_count))
FROM test_kylin_fact
GROUP BY (sinh(price) - sinh(item_count)) / (sinh(cast(price AS bigint) + item_count)),
         price,
         item_count
ORDER BY price,
         item_count LIMIT 10;