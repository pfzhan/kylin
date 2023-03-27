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
-- Unless required by applicable law or agreed to in writing, softwarea
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


select SELLER_ID,kcg.CATEG_LVL3_ID,kcg2.CATEG_LVL3_ID,sum(price),count(distinct TEST_KYLIN_FACT.ORDER_ID)
from TEST_KYLIN_FACT
inner join TEST_ORDER on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID
inner join TEST_CATEGORY_GROUPINGS as tcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = tcg.LEAF_CATEG_ID
inner join KYLIN_CATEGORY_GROUPINGS as kcg on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg.LEAF_CATEG_ID
left join KYLIN_CATEGORY_GROUPINGS as kcg2 on TEST_KYLIN_FACT.LEAF_CATEG_ID = kcg2.LEAF_CATEG_ID
group by SELLER_ID,kcg.CATEG_LVL3_ID,kcg2.CATEG_LVL3_ID
order by SELLER_ID,kcg.CATEG_LVL3_ID desc limit 9

