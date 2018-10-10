select t2.cnt_1, t2.week_beg_dt, t2.sum_price from (
SELECT t1.cnt_1, t1.sum_price, t1.week_beg_dt
 FROM (
 select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as sum_price, count(1) as cnt_1
 from test_kylin_fact
inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
 group by test_cal_dt.week_beg_dt
 ) t1
)t2