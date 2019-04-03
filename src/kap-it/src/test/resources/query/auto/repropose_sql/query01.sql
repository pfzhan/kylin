

select test_kylin_fact.order_id, lstg_format_name
from test_kylin_fact left join test_order on test_kylin_fact.order_id = test_order.order_id
order by test_kylin_fact.order_id, lstg_format_name
limit 10
