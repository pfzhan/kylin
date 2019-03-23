-- delete min(1), max(1), max(N) Pattern in DefaultQueryTransformer
-- #10739   (min/max fixed in CALCITE-1436; having count(1) > 0 fixed in CALCITE-1306)

select min(1), max(1), max(3.0), count(*) from test_kylin_fact

