--#13340
select case when substring(lstg_format_name, 1, 4) in ('ABIN', 'ABC') then item_count - 10 else item_count end as  item_count_new from test_kylin_fact