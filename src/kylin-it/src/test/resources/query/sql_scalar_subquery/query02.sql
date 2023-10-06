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


SELECT D_DATEKEY,
       DATE_TIME,
       sum(D_YEAR) total
FROM (SELECT '1995-03-01' AS DATE_TIME
      UNION ALL
      SELECT '1995-03-02' AS DATE_TIME
      UNION ALL
      SELECT '1995-03-03' AS DATE_TIME) dt
         LEFT JOIN SSB.DATES t2 ON dt.DATE_TIME = t2.D_DATEKEY
GROUP BY D_DATEKEY,
         DATE_TIME