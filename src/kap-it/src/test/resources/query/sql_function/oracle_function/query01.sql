--KE-12166
-- need config kylin.query.calcite.extras-props.FUN=standard,oracle
select nvl(lstg_format_name, 'a'), ltrim(lstg_format_name), rtrim(lstg_format_name)
--,substr(lstg_format_name,4)
 from TEST_KYLIN_FACT