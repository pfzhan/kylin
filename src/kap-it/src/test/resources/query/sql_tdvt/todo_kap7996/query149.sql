SELECT {fn LTRIM({fn CONVERT({fn YEAR("CALCS"."DATE2")}, SQL_VARCHAR)})} AS "TEMP_Test__1344687376__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn LTRIM({fn CONVERT({fn YEAR("CALCS"."DATE2")}, SQL_VARCHAR)})}