SELECT {fn HOUR({fn CONVERT("CALCS"."DATETIME0", SQL_TIMESTAMP)})} AS "TEMP_Test__238882764__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn HOUR({fn CONVERT("CALCS"."DATETIME0", SQL_TIMESTAMP)})}