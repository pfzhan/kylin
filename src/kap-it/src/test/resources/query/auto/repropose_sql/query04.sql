

select lstg_format_name, test_kylin_fact.leaf_categ_id, sum(price)
from test_kylin_fact inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id
group by lstg_format_name, test_kylin_fact.leaf_categ_id
order by lstg_format_name, test_kylin_fact.leaf_categ_id
limit 10