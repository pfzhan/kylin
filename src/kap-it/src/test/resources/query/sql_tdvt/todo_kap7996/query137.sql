SELECT {fn LTRIM({fn CONVERT({fn CONVERT({fn TRUNCATE((6 + {fn DAYOFYEAR("CALCS"."DATE2")} + ({fn MOD((7 + {fn DAYOFWEEK({fn TIMESTAMPADD(SQL_TSI_DAY,{fn CONVERT({fn TRUNCATE((-1 * ({fn DAYOFYEAR("CALCS"."DATE2")} - 1)),0)}, SQL_BIGINT)},{fn CONVERT("CALCS"."DATE2", SQL_DATE)})})} - 2 ), 7)})) / 7,0)}, SQL_BIGINT)}, SQL_VARCHAR)})} AS "TEMP_Test__641609351__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn LTRIM({fn CONVERT({fn CONVERT({fn TRUNCATE((6 + {fn DAYOFYEAR("CALCS"."DATE2")} + ({fn MOD((7 + {fn DAYOFWEEK({fn TIMESTAMPADD(SQL_TSI_DAY,{fn CONVERT({fn TRUNCATE((-1 * ({fn DAYOFYEAR("CALCS"."DATE2")} - 1)),0)}, SQL_BIGINT)},{fn CONVERT("CALCS"."DATE2", SQL_DATE)})})} - 2 ), 7)})) / 7,0)}, SQL_BIGINT)}, SQL_VARCHAR)})}