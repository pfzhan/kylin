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
SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT SUM(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
		ORDER BY leaf_categ_id
		limit 1
	)
UNION ALL
SELECT cast(1999 as bigint) AS leaf_categ_id, 11.2 AS sum_price, 21.2 AS sum_price2
UNION ALL
SELECT *
FROM (
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
	UNION ALL
	SELECT leaf_categ_id, SUM(price) AS sum_price
	FROM test_kylin_fact
	GROUP BY leaf_categ_id
)
	CROSS JOIN (
		SELECT SUM(price) AS sum_price_2
		FROM test_kylin_fact
		GROUP BY leaf_categ_id
		ORDER BY leaf_categ_id
		limit 1
	)