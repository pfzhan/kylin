select  find_in_set('dab', rpad(lpad(lstg_format_name, cast ('15' as  tinyint) - 1, 'abc,b,c,d'),   25 ,'abc,b,c,d') )  from TEST_KYLIN_FACT
