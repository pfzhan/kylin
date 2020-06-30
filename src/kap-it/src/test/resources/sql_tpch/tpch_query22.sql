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
with q22_customer_tmp_cached as (
select
	c_acctbal,
	c_custkey,
	substring(c_phone, 1, 2) as cntrycode
from
	tpch.customer
where
	substring(c_phone, 1, 2) = '13' or
	substring(c_phone, 1, 2) = '31' or
	substring(c_phone, 1, 2) = '23' or
	substring(c_phone, 1, 2) = '29' or
	substring(c_phone, 1, 2) = '30' or
	substring(c_phone, 1, 2) = '18' or
	substring(c_phone, 1, 2) = '17'
 ),

q22_customer_tmp1_cached as (
select
	avg(c_acctbal) as avg_acctbal
from
	q22_customer_tmp_cached
where
	c_acctbal > 0.00
),
q22_orders_tmp_cached as (
select
	o_custkey
from
	tpch.orders
group by
	o_custkey
)

select
	cntrycode,
	count(1) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		cntrycode,
		c_acctbal,
		avg_acctbal
	from
		q22_customer_tmp1_cached ct1, (
			select
				cntrycode,
				c_acctbal
			from
				q22_orders_tmp_cached ot
				right outer join q22_customer_tmp_cached ct
				on ct.c_custkey = ot.o_custkey
			where
				o_custkey is null
		) ct2
) as a
where
	c_acctbal > avg_acctbal
group by
	cntrycode
order by
	cntrycode;
