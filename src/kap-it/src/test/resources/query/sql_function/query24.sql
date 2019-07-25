-- https://github.com/Kyligence/KAP/issues/13613

SELECT ORDER_ID,CASE substr(lstg_format_name,1,4)
              WHEN 'A'     THEN 'begin with A'
              WHEN 'B'     THEN 'begin with B'
              WHEN 'C'     THEN 'begin with C'
              ELSE 'begin with other' END
FROM test_kylin_fact
order by ORDER_ID