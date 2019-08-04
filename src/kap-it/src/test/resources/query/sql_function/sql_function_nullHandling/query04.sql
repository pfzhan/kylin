-- https://github.com/Kyligence/KAP/issues/13615

select ifnull(ID2,132322342)
from TEST_MEASURE
group by ID2