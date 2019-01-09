--https://github.com/Kyligence/KAP/issues/9221

select cal_dt, count(1) from test_kylin_fact group by cal_dt