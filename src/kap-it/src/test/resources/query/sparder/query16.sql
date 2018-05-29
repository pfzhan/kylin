select slr_segment_cd as x  ,sum(price) as GMV from test_kylin_fact
 group by  slr_segment_cd