-- #13340
select sum({fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}) from test_kylin_fact group by lstg_format_name