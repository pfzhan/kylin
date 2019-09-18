

SELECT expm1(price),
       max(expm1(abs(price * item_count) + 1))
FROM test_kylin_fact
WHERE expm1(price) > 0
GROUP BY price,
         item_count
ORDER BY price LIMIT 10