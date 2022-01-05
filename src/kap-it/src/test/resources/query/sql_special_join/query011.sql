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
SELECT COUNT("T"."ACCOUNT_COUNTRY"), "ACCOUNT_SELLER_LEVEL"
FROM (
	SELECT "PRICE", "TRANS_ID", "SELLER_ID" FROM "TEST_KYLIN_FACT" ORDER BY "TRANS_ID" DESC
	)
"S" INNER JOIN (
	SELECT "ACCOUNT_COUNTRY", "ACCOUNT_ID", "ACCOUNT_SELLER_LEVEL" FROM "TEST_ACCOUNT" ORDER BY "ACCOUNT_ID" ASC
	)
"T"
ON "T"."ACCOUNT_ID" = "S"."SELLER_ID"
INNER JOIN "TEST_COUNTRY" "E"
ON "T"."ACCOUNT_COUNTRY" = "E"."COUNTRY"
GROUP BY "ACCOUNT_SELLER_LEVEL"
ORDER BY ACCOUNT_SELLER_LEVEL