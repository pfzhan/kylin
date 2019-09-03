--https://github.com/Kyligence/KAP/issues/14074

select *
from TEST_MEASURE
where ifnull(time2,timestamp'2019-08-08 16:33:41.061')  =  timestamp'2019-08-08 16:33:41.061'