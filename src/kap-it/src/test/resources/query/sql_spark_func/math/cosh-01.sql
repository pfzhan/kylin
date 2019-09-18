


SELECT cosh(0),
       cosh(price)
FROM test_kylin_fact
WHERE price > 10
  AND price < 50
ORDER BY price LIMIT 10;