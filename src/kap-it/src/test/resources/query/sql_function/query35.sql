select overlay(LSTG_FORMAT_NAME PLACING 'abc' FROM 1),
       overlay(LSTG_FORMAT_NAME PLACING 'abc' FROM 1 FOR 1)
 from test_kylin_fact