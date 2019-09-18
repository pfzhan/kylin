


SELECT (tanh(price) - tanh(item_count)) / (tanh(cast(price AS bigint) + item_count))
FROM test_kylin_fact
GROUP BY (tanh(price) - tanh(item_count)) / (tanh(cast(price AS bigint) + item_count)),
         price,
         item_count
ORDER BY price,
         item_count LIMIT 10;