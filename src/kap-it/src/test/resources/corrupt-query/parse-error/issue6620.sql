--https://github.com/Kyligence/KAP/issues/6620

select function(test_time_enc) as h1, count(*) as cnt
from test_kylin_fact as test_kylin_fact
inner join test_order as test_order on test_kylin_fact.order_id = test_order.order_id
group by test_time_enc