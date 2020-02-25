-- KE#11832
select  {fn IFNULL(lstg_format_name, '')} AS "TEMP_Test__976753306__0_"
FROM test_kylin_fact
group by {fn IFNULL(lstg_format_name, '')}