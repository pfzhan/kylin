-- https://github.com/Kyligence/KAP/issues/13615

select ifnull(ID2,123)
from TEST_MEASURE
group by ID2