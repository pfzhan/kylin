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
--#6520
--failed in sparder
--failed if replace line 22 to 'FROM TDVT.CALCS CALCS'
SELECT Sta.cal_dt AS DAT, sum(Sta.PRICE) AS PRI, COUNT(*) AS COU
--    FROM TDVT.CALCS CALCS
    FROM (select cal_dt FROM test_kylin_fact) as CALCS
    CROSS JOIN
        (    SELECT cal_dt, PRICE
             FROM (SELECT 1) as temp
             CROSS JOIN test_kylin_fact
        ) as Sta
GROUP BY Sta.cal_dt
ORDER BY Sta.cal_dt

