

SELECT hypot(3, 4),
       hypot(price, 2)
FROM test_kylin_fact
ORDER BY price LIMIT 1;