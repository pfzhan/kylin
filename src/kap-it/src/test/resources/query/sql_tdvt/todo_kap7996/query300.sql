SELECT (CASE WHEN ({fn YEAR({fn CURRENT_DATE()})} = {fn YEAR({fn CURRENT_DATE()})}) THEN 1 WHEN NOT ({fn YEAR({fn CURRENT_DATE()})} = {fn YEAR({fn CURRENT_DATE()})}) THEN 0 ELSE NULL END) AS "TEMP_Test__307093745__0_",
  1 AS "X__alias__0"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY 1