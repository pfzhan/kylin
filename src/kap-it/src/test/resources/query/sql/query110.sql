select d1.lstg_format_name from
  (select * from test_kylin_fact where test_kylin_fact.seller_id in ( 10000002, 10000003, 10000004,10000005,10000006,10000008,10000009,10000001,10000010,10000011) ) d1
  cross join
  (select * from test_kylin_fact where test_kylin_fact.seller_id in ( 10000002, 10000003, 10000004,10000005,10000006,10000008,10000009,10000001,10000010,10000011)) d2