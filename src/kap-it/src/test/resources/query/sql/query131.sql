-- https://github.com/Kyligence/KAP/issues/14474

SELECT cast(id1 AS double ),
       cast(id2 AS float),
       cast(id3 AS bigint),
       cast(id4 AS int),
       cast(price1 AS float),
       cast(price2 AS double),
       cast(price3 AS decimal(19,6)),
       cast(price5 AS double ),
       cast(price6 AS tinyint),
       cast(price7 AS smallint),
       cast(name1 AS varchar),
       cast(name2 AS varchar(254)),
       cast(name3 AS char),
       cast(name4 AS tinyint),
       cast(time1 AS date),
       cast(time2 AS TIMESTAMP),
       cast(flag AS boolean)
FROM test_measure