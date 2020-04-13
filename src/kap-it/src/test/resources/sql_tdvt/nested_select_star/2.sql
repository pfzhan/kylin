-- check this query should be ok , which result cannot be contain measure field
with t1 as (select * from "TDVT"."CALCS") select * from t1 order by KEY limit 1;