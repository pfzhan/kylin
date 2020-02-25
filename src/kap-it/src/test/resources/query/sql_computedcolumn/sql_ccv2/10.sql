-- KE#11832
-- multi groupby cc (diff with rewrite)
select {fn IFNULL(lstg_format_name, '')}
FROM test_kylin_fact
group by {fn IFNULL(lstg_format_name, '')}, CAL_DT, {fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}