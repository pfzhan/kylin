select LSTG_FORMAT_NAME as z,slr_segment_cd as x ,sum(price) as GMV, count(1) as TRANS_CNT from test_kylin_fact
 group by LSTG_FORMAT_NAME ,slr_segment_cd