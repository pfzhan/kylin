SELECT {fn DAYOFMONTH({fn CONVERT("CALCS"."DATE2", SQL_DATE)})} AS "TEMP_Test__379388560__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn DAYOFMONTH({fn CONVERT("CALCS"."DATE2", SQL_DATE)})}