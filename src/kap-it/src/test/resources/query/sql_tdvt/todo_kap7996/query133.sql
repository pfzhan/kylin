SELECT {fn LTRIM({fn CONVERT({fn QUARTER("CALCS"."DATETIME0")}, SQL_VARCHAR)})} AS "TEMP_Test__137257054__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn LTRIM({fn CONVERT({fn QUARTER("CALCS"."DATETIME0")}, SQL_VARCHAR)})}