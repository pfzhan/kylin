


SELECT price,
       tanh(0.0),
       tanh(price)
FROM test_kylin_fact
GROUP BY price,
         tanh(price)
ORDER BY price LIMIT 2;