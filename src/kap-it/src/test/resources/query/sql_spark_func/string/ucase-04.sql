select   count(ucase(substring(substring_index(concat(lstg_format_name, '.www.apache.org'), '.', 1 + 1), 2, 1+3)))
from test_kylin_fact
group by  ucase(substring(substring_index(concat(lstg_format_name, '.www.apache.org'), '.', cast ('2' as  tinyint) ), 1, 1+2))