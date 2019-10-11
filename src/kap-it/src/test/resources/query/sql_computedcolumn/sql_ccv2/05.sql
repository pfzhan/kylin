SELECT sum(item_count)
FROM test_kylin_fact
group by {fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}