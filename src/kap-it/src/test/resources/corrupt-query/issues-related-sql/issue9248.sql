--https://github.com/Kyligence/KAP/issues/9248

select * from (
  select row_number() over(partition by seller_id order by cal_dt desc) as row_num, trans_id, seller_id, cal_dt
  from test_kylin_fact
) test_kylin_fact