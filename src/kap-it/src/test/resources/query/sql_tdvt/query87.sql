SELECT {fn TIMESTAMPDIFF(SQL_TSI_DAY,"CALCS"."DATE3","CALCS"."DATE2")} AS "TEMP_Test__1975326027__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_DAY,"CALCS"."DATE3","CALCS"."DATE2")}