


SELECT rint(price + 1.223),
       rint(price)
FROM test_kylin_fact
ORDER BY price LIMIT 10;