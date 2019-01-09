--https://github.com/Kyligence/KAP/issues/7180

select cal_dt, is_effectual, leaf_categ_id, lstg_format_name, lstg_site_id, order_id, price, seller_id, slr_segment_cd, trans_id
from test_kylin_fact
order by price
limit 200