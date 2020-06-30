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
SELECT "Z_360_HEAT"."GAP_NAME" AS "GAP_NAME",
"Z_360_HEAT"."HCC_NAME" AS "HCC_NAME",
"Z_360_HEAT"."MEMBER_ID" AS "MEMBER_ID",
"Z_360_HEAT"."MEMBER_NAME" AS "MEMBER_NAME",
"Z_360_HEAT"."MRN" AS "MRN",
"Z_360_HEAT"."PAYER_LOB" AS "PAYER_LOB",
AVG("Z_360_HEAT"."INCREMENTAL_RAF") AS "avg_INCREMENTAL_RAF_ok",
((({fn YEAR("Z_360_HEAT"."MEMBER_DOB")} * 10000) + ({fn MONTH("Z_360_HEAT"."MEMBER_DOB")} * 100))
+ {fn DAYOFMONTH("Z_360_HEAT"."MEMBER_DOB")}) AS "md_MEMBER_DOB_ok"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
WHERE (("Z_360_HEAT"."PCP_NAME" = 'JONATHAN AREND')
AND ("Z_360_HEAT"."GAP_CLOSED" = 'No')
AND ("Z_360_HEAT"."GAP_TYPE" = 'HCC SUSPECT'))
GROUP BY "Z_360_HEAT"."GAP_NAME", "Z_360_HEAT"."HCC_NAME", "Z_360_HEAT"."MEMBER_ID",
"Z_360_HEAT"."MEMBER_NAME", "Z_360_HEAT"."MRN", "Z_360_HEAT"."PAYER_LOB",
((({fn YEAR("Z_360_HEAT"."MEMBER_DOB")} * 10000)
+ ({fn MONTH("Z_360_HEAT"."MEMBER_DOB")} * 100)) + {fn DAYOFMONTH("Z_360_HEAT"."MEMBER_DOB")});