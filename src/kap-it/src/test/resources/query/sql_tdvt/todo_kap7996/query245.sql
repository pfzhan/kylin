SELECT {fn TIMESTAMPADD(SQL_TSI_SECOND,{fn SECOND("CALCS"."DATETIME0")},{fn TIMESTAMPADD(SQL_TSI_MINUTE,{fn MINUTE("CALCS"."DATETIME0")},{fn TIMESTAMPADD(SQL_TSI_HOUR,{fn HOUR("CALCS"."DATETIME0")},{fn CONVERT("CALCS"."DATETIME0", SQL_DATE)})})})} AS "TEMP_Test__2064041557__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn TIMESTAMPADD(SQL_TSI_SECOND,{fn SECOND("CALCS"."DATETIME0")},{fn TIMESTAMPADD(SQL_TSI_MINUTE,{fn MINUTE("CALCS"."DATETIME0")},{fn TIMESTAMPADD(SQL_TSI_HOUR,{fn HOUR("CALCS"."DATETIME0")},{fn CONVERT("CALCS"."DATETIME0", SQL_DATE)})})})}