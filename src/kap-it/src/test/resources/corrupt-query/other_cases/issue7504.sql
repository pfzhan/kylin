--https://github.com/Kyligence/KAP/issues/7504

select o.user_defined_field1, p.user_defined_field1
from test_kylin_fact f
inner join test_category_groupings o on f.leaf_categ_id = o.leaf_categ_id
inner join test_category_groupings p on f.leaf_categ_id = p.leaf_categ_id