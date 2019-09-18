


SELECT price,
       item_count
FROM test_kylin_fact
WHERE cosh(price * item_count) > 0
  AND cosh(price * item_count) < 10000
GROUP BY price,
         item_count
ORDER BY price,
         item_count LIMIT 12;