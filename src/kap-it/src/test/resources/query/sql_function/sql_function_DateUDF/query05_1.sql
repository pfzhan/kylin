select
    ceil(timestamp '2012-03-31 23:59:59.888' to second ),
    ceil(timestamp '2012-03-31 23:59:59.888' to minute ),
    ceil(ceil(timestamp '2012-03-31 23:59:59.888' to hour ) to minute ),
    ceil(timestamp '2012-03-31 23:59:59.888' to hour ),
    ceil(ceil(timestamp '2012-03-31 23:59:59.888' to hour ) to hour),
    ceil(timestamp '2012-03-31 00:01:59.12' to day ),
    ceil(timestamp '2012-02-29 00:00:00.0' to day ),
    ceil(timestamp '2012-12-31 23:59:59.888' to month ),
    ceil(timestamp '2012-12-31 23:59:59.888' to year ),
    --ceil(ceil(date '2013-03-31' to HOUR) to HOUR), -- calcite not support ceil(date_type to timeunit)
    ceil(timestamp '2013-03-31 00:00:00' to hour ),
    floor(timestamp '2013-03-31 00:00:00' to hour )
from test_measure limit 1