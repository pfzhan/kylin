SELECT {fn LTRIM({fn CONVERT({fn QUARTER("CALCS"."DATE2")}, SQL_VARCHAR)})} AS "TEMP_Test__2341393388__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn LTRIM({fn CONVERT({fn QUARTER("CALCS"."DATE2")}, SQL_VARCHAR)})}