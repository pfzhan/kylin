select t1.leaf_categ_id, max(max_price) as x, min(min_price) as y , sum(sum_price) as z
from
(select leaf_categ_id, sum(price) as sum_price from test_kylin_fact group by leaf_categ_id) t1
join
(select leaf_categ_id, max(price) as max_price from test_kylin_fact group by leaf_categ_id) t2
on t1.leaf_categ_id = t2.leaf_categ_id
join
(select leaf_categ_id, min(price) as min_price from test_kylin_fact group by leaf_categ_id) t3
on t1.leaf_categ_id = t3.leaf_categ_id  group by t1.leaf_categ_id