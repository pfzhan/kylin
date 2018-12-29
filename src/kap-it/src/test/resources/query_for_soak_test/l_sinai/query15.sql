SELECT COUNT(DISTINCT "Z_PROVDASH_UM_ED"."MEMBER_ID") AS "ctd_MEMBER_ID_ok",
SUM({fn CONVERT(0, SQL_BIGINT)}) AS "sum_Calculation_336925569152049156_ok"
FROM "POPHEALTH_ANALYTICS"."Z_PROVDASH_UM_ED" "Z_PROVDASH_UM_ED"
WHERE ("Z_PROVDASH_UM_ED"."FULL_NAME" = 'JONATHAN AREND')
HAVING (COUNT(1) > 0);