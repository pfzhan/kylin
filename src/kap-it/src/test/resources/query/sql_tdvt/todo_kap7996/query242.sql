SELECT {fn TIMESTAMPADD(SQL_TSI_MONTH,{fn CONVERT({fn TRUNCATE((3 * ({fn CONVERT({fn TRUNCATE({fn QUARTER("CALCS"."DATE2")},0)}, SQL_BIGINT)} - 1)),0)}, SQL_BIGINT)},{fn TIMESTAMPADD(SQL_TSI_DAY,{fn CONVERT({fn TRUNCATE((-1 * ({fn DAYOFYEAR("CALCS"."DATE2")} - 1)),0)}, SQL_BIGINT)},{fn CONVERT("CALCS"."DATE2", SQL_DATE)})})} AS "TEMP_Test__1201787268__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn TIMESTAMPADD(SQL_TSI_MONTH,{fn CONVERT({fn TRUNCATE((3 * ({fn CONVERT({fn TRUNCATE({fn QUARTER("CALCS"."DATE2")},0)}, SQL_BIGINT)} - 1)),0)}, SQL_BIGINT)},{fn TIMESTAMPADD(SQL_TSI_DAY,{fn CONVERT({fn TRUNCATE((-1 * ({fn DAYOFYEAR("CALCS"."DATE2")} - 1)),0)}, SQL_BIGINT)},{fn CONVERT("CALCS"."DATE2", SQL_DATE)})})}