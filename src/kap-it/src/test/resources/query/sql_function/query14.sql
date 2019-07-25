-- https://github.com/Kyligence/KAP/issues/13613

--test null. TEST_MEASURE.name1 contain null value
select substr(name1,2) a from
TEST_MEASURE