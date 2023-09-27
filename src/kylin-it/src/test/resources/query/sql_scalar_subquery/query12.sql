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


SELECT DATE_TIME,
       count(D_HOLIDAYFL, LO_CUSTKEY),
       count(D_HOLIDAYFL),
       count(*),
       count(1),
       count(LO_CUSTKEY),
       sum(D_YEAR)     sy,
       sum(LO_REVENUE) sr
FROM (SELECT '1995-03-01' AS DATE_TIME
      UNION ALL
      SELECT '2050-03-02' AS DATE_TIME
      UNION ALL
      SELECT '1995-03-02' AS DATE_TIME
      UNION ALL
      SELECT '1995-03-02' AS DATE_TIME
      UNION ALL
      SELECT '1995-03-03' AS DATE_TIME) t1
         LEFT JOIN SSB.DATES t2
                   ON t2.D_DATE <= t1.DATE_TIME
                       AND t2.D_DATE >= CONCAT(SUBSTR(t1.DATE_TIME, 1, 8), '01')
         INNER JOIN SSB.LINEORDER t3 ON t3.LO_ORDERDATE = t1.DATE_TIME
GROUP BY DATE_TIME
