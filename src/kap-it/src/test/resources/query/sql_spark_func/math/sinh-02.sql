


SELECT price,
       sinh(0.0),
       sinh(price* 3)
FROM test_kylin_fact
GROUP BY price,
         sinh(price*3)
ORDER BY price LIMIT 2;