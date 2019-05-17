SELECT SUM({fn CONVERT(1, SQL_BIGINT)}) AS "sum_Number_of_Records_ok"
FROM "TDVT"."CALCS" "Calcs"
WHERE ('All' = 'All')
HAVING (COUNT(1) > 0)