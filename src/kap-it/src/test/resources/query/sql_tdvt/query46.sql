SELECT {fn CONVERT({fn LTRIM({fn CONVERT("CALCS"."DATE0", SQL_VARCHAR)})}, SQL_DATE)} AS "TEMP_Test__1894730482__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn CONVERT({fn LTRIM({fn CONVERT("CALCS"."DATE0", SQL_VARCHAR)})}, SQL_DATE)}