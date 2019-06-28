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
select LSTG_FORMAT_NAME,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday
from test_kylin_fact
where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03')
group by LSTG_FORMAT_NAME