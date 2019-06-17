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
-- copy from sql_cross_join/query04.sql
-- ISSUE #5613
--failed if replace line 24 to 'FROM TDVT.CALCS CALCS'
--failed 'type mismatch:BIGINT NOT NULL' in left join
--if append ', COUNT(*) AS COU' to line 21
SELECT Sta.datetime0 AS DAT, sum(Sta.NUM4) AS PRI
, COUNT(*) AS COU
--    FROM TDVT.CALCS CALCS
    FROM (select datetime0 FROM TDVT.CALCS CALCS) as CALCS
    INNER JOIN
        (    SELECT CALCS.datetime0, CALCS.NUM4
             FROM TDVT.CALCS CALCS
             INNER JOIN (   select datetime0
                            FROM TDVT.CALCS CA
                            group by datetime0)
                        as CA
             ON CALCS.datetime0 = CA.datetime0
        ) as Sta
    ON CALCS.datetime0 = Sta.datetime0
GROUP BY Sta.datetime0
ORDER BY Sta.datetime0
