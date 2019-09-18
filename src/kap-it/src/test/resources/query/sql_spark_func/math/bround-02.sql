 -- https://github.com/Kyligence/KAP/issues/14852
-- param with column

SELECT bround(price, 0),
       bround(price * item_count, 1)
FROM test_kylin_fact
GROUP BY bround(price, 0),
         bround(price * item_count, 1);