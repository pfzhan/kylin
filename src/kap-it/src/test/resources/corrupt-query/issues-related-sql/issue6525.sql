--https://github.com/Kyligence/KAP/issues/6525

select test_kylin_fact.cal_dt, seller_id
from test_kylin_fact
inner join edw.test_cal_dt as test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt
inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id
  and test_kylin_fact.lstg_site_id = test_category_groupings.site_id
group by test_kylin_fact.cal_dt, test_kylin_fact.seller_id
order by sum(test_kylin_fact.price) desc limit 20