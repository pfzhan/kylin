SELECT {fn TIMESTAMPDIFF(SQL_TSI_MINUTE,{fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)},"CALCS"."DATETIME0")} AS "TEMP_Test__2635966198__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_MINUTE,{fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)},"CALCS"."DATETIME0")}