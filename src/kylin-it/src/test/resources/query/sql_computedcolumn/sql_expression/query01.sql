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


select CAL_DT,
       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 end),
       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else 3 end),
       sum(case when ORDER_ID in (1, 2) then 2 else 3 end),
       sum(case when PRICE in (1.1, 2.2) then 2 else 3 end)
from TEST_KYLIN_FACT
group by CAL_DT
