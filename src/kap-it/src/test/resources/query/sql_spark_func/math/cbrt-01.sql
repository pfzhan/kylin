-- https://github.com/Kyligence/KAP/issues/14852
 --table index test

SELECT cbrt(27.0),
       cbrt(-27.0),
       cbrt(cast('12323223' as bigint))
FROM test_kylin_fact
WHERE cbrt(price) > 2 LIMIT 2;