SELECT {fn YEAR({fn TIMESTAMPADD(SQL_TSI_DAY,((-{fn DAYOFWEEK("CALCS"."DATE0")}) + 1),"CALCS"."DATE0")})} AS "TEMP_Test__649587318__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn YEAR({fn TIMESTAMPADD(SQL_TSI_DAY,((-{fn DAYOFWEEK("CALCS"."DATE0")}) + 1),"CALCS"."DATE0")})}