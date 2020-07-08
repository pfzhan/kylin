-- KE14922 support WVARCHAR, SQL_WVARCHAR by calcite

select
    cast( "LSTG_FORMAT_NAME" as WVARCHAR ),
    { fn convert( "LSTG_FORMAT_NAME", WVARCHAR ) },
    { fn convert( "LSTG_FORMAT_NAME", SQL_WVARCHAR ) }
from test_kylin_fact