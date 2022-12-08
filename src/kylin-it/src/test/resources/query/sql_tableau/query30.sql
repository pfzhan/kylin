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

SELECT
  "KYLIN_SALES"."LSTG_FORMAT_NAME" AS "LSTG_FORMAT_NAME",
  SUM(
    { fn DEGREES(
      (
        CASE
          WHEN ("KYLIN_SALES"."PRICE" + 2) = 0 THEN { fn CONVERT(NULL, SQL_DOUBLE) }
          ELSE "KYLIN_SALES"."PRICE" / ("KYLIN_SALES"."PRICE" + 2)
        END
      )
    ) }
  ) AS "sum_Calculation_4185251491127156773_ok"
FROM
  "DEFAULT"."TEST_KYLIN_FACT" "KYLIN_SALES"
    INNER JOIN "DEFAULT"."TEST_CATEGORY_GROUPINGS" "KYLIN_CATEGORY_GROUPINGS" ON (
      (
        "KYLIN_SALES"."LEAF_CATEG_ID" = "KYLIN_CATEGORY_GROUPINGS"."LEAF_CATEG_ID"
      )
      AND (
        "KYLIN_SALES"."LSTG_SITE_ID" = "KYLIN_CATEGORY_GROUPINGS"."SITE_ID"
      )
    )
    INNER JOIN "EDW"."TEST_CAL_DT" "KYLIN_CAL_DT" ON (
      "KYLIN_SALES"."CAL_DT" = "KYLIN_CAL_DT"."CAL_DT"
    )
    INNER JOIN "DEFAULT"."TEST_ACCOUNT" "SELLER_ACCOUNT" ON (
      "KYLIN_SALES"."SELLER_ID" = "SELLER_ACCOUNT"."ACCOUNT_ID"
    )
GROUP BY
  "KYLIN_SALES"."LSTG_FORMAT_NAME"
