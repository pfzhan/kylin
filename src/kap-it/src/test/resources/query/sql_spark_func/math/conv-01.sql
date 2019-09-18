-- https://github.com/Kyligence/KAP/issues/14852
 --table index test

SELECT conv('100', 2, 10),
       conv(-10, 16, 10)
FROM test_kylin_fact
WHERE conv(seller_id, 10, 16) = 989680
ORDER BY seller_id LIMIT 2;