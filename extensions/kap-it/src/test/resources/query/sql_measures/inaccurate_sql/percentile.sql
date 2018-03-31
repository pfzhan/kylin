select percentile_approx(PRICE1, 0.9), percentile_approx(ID2, 0.23), percentile(PRICE3, 0.49)
from TEST_MEASURE GROUP BY ID1