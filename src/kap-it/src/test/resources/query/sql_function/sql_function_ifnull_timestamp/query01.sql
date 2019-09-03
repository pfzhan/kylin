--https://github.com/Kyligence/KAP/issues/13977

select *
from TEST_MEASURE
where ifnull(time2,CURRENT_TIMESTAMP)  =  CURRENT_TIMESTAMP  --test timestamp null value