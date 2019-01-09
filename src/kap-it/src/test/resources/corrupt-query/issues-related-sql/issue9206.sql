--https://github.com/Kyligence/KAP/issues/9206

-- select OPS_REGION from kylin_sales
-- where OPS_REGION ='Hongkong' or OPS_REGION ='Shanghai'
-- group by OPS_REGION

select lstg_format_name
from test_kylin_fact
where lstg_format_name = 'xxx' or lstg_format_name = 'yyy'
group by lstg_format_name

