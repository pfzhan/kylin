


SELECT (cosh(price) - cosh(item_count)) / (cosh(cast(price AS bigint) + item_count))
FROM test_kylin_fact
GROUP BY (cosh(price) - cosh(item_count)) / (cosh(cast(price AS bigint) + item_count)),
         price,
         item_count
ORDER BY price,
         item_count LIMIT 10;