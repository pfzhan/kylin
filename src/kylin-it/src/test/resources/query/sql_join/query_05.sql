--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

select OTBL.PRICE, ITBL.C1
from (
    select SELLER_ID, cast(PRICE+1.0 as double) PRICE, CAL_DT from test_kylin_fact
) as OTBL
left join (
    select ACCOUNT_ID, trim(ACCOUNT_COUNTRY) as C1
    from TEST_ACCOUNT
) as ITBL
on OTBL.SELLER_ID = ITBL.ACCOUNT_ID
order by OTBL.PRICE asc
LIMIT 10
