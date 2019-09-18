---
---
---  tanh = (e^(x) - e^(-x)) / (e^(x) + e^(-x)), x can be any real number
---
---
SELECT tanh(0.0),
       tanh(price * 2)
FROM test_kylin_fact
ORDER BY price LIMIT 2