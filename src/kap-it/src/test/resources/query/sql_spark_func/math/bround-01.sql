-- https://github.com/Kyligence/KAP/issues/14852
-- constant testing

SELECT bround(1002.5786, -3),
       bround(-10.8, 0),
       bround(125.0 / 9, 5) --,bround(125 / 9, 5)
FROM test_kylin_fact
WHERE bround(price, 1) > 0 LIMIT 2;