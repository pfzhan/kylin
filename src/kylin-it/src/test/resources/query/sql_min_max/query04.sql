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

select *
from (SELECT MIN(LO_ORDERKEY) AS MIN_KEY, MAX(LO_ORDERKEY) AS MAX_KEY, 1.001 AS CONST
      FROM SSB.LINEORDER
               INNER JOIN SSB.DATES
                          ON LINEORDER.LO_ORDERDATE = DATES.D_DATEKEY) t1
         LEFT JOIN (SELECT MIN(LO_ORDERKEY) AS MIN_KEY, MAX(LO_ORDERKEY) AS MAX_KEY, 1.0001 AS CONST
                    FROM SSB.LINEORDER
                    GROUP BY 1.0001) T ON T.MIN_KEY = t1.MIN_KEY
