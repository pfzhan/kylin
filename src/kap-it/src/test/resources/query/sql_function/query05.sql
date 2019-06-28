--- support pi, kylin.query.calcite.extras-props.conformance=DEFAULT, however hive and spark-sql only support pi();
--- if need support pi(), kylin.query.calcite.extras-props.conformance=LENIENT

select pi from test_kylin_fact limit 2