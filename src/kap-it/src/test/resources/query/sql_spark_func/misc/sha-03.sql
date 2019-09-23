select lstg_format_name , substring(sha(substring_index(concat(lstg_format_name, '.www.apache.org'), '.', cast ('2' as  tinyint) )), 1, 1+2) from  test_kylin_fact
