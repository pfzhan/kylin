SELECT (CASE WHEN ({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)} > "CALCS"."DATETIME0") THEN 1 WHEN NOT ({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)} > "CALCS"."DATETIME0") THEN 0 ELSE NULL END) AS "TEMP_Test__3925608778__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY (CASE WHEN ({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)} > "CALCS"."DATETIME0") THEN 1 WHEN NOT ({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)} > "CALCS"."DATETIME0") THEN 0 ELSE NULL END)