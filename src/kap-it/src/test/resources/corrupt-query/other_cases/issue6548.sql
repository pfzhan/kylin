--https://github.com/Kyligence/KAP/issues/6548

select test_kylin_fact.lstg_format_name,sum(test_kylin_fact.price) as gmv, count(*) as trans_cnt
from test_kylin_fact
inner join (select cal_dt,week_beg_dt from edw.test_cal_dt where week_beg_dt >= date '2012-04-10' ) xxx on test_kylin_fact.cal_dt = xxx.cal_dt
inner join test_category_groupings on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id and test_kylin_fact.lstg_site_id = test_category_groupings.site_id
inner join (select cal_dt,week_beg_dt from edw.test_cal_dt where week_beg_dt >= date '2013-01-01' ) xxx2 on test_kylin_fact.cal_dt = xxx2.cal_dt
where test_category_groupings.meta_categ_name <> 'Baby'
group by test_kylin_fact.lstg_format_name