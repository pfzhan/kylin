--https://github.com/Kyligence/KAP/issues/6594

select week_beg_dt as week,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['FP-GTC']) as a,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['Auction']) as b,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['Others']) as c,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['FP-GTC', 'Auction']) as ab,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['FP-GTC', 'Others']) as ac,
intersect_count( test_count_distinct_bitmap, lstg_format_name, array['FP-GTC', 'Auction', 'Others']) as abc,
count(distinct test_count_distinct_bitmap) as sellers,
count(*) as cnt
from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt
where week_beg_dt in (date '2013-12-22', date '2012-06-23')
group by week_beg_dt