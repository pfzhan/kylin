--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--

SELECT "KYLIN_CATEGORY_GROUPINGS"."CATEG_LVL3_NAME" AS "CATEG_LVL3_NAME",   COUNT(1) AS "usr__COUNT__ok",
  {fn CONVERT("KYLIN_CAL_DT"."RETAIL_START_DATE", SQL_DATE)} AS "yr_RETAIL_START_DATE_ok"
FROM "DEFAULT"."TEST_KYLIN_FACT" "KYLIN_SALES"
INNER JOIN "EDW"."TEST_CAL_DT" "KYLIN_CAL_DT" ON ("KYLIN_SALES"."CAL_DT" = "KYLIN_CAL_DT"."CAL_DT")
INNER JOIN "DEFAULT"."TEST_CATEGORY_GROUPINGS" "KYLIN_CATEGORY_GROUPINGS"
  ON (("KYLIN_SALES"."LEAF_CATEG_ID" = "KYLIN_CATEGORY_GROUPINGS"."LEAF_CATEG_ID")
  AND ("KYLIN_SALES"."LSTG_SITE_ID" = "KYLIN_CATEGORY_GROUPINGS"."SITE_ID"))
INNER JOIN "DEFAULT"."TEST_ACCOUNT" "SELLER_ACCOUNT" ON ("KYLIN_SALES"."SELLER_ID" = "SELLER_ACCOUNT"."ACCOUNT_ID")
INNER JOIN "DEFAULT"."TEST_COUNTRY" "SELLER_COUNTRY" ON ("SELLER_ACCOUNT"."ACCOUNT_COUNTRY" = "SELLER_COUNTRY"."COUNTRY")
GROUP BY "KYLIN_CATEGORY_GROUPINGS"."CATEG_LVL3_NAME",   {fn CONVERT("KYLIN_CAL_DT"."RETAIL_START_DATE", SQL_DATE)}