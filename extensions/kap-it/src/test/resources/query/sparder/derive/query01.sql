  select sum(price) as GMV, count(seller_id) as TRANS_CNT
 from test_kylin_fact where test_kylin_fact.lstg_format_name ='ABIN'
