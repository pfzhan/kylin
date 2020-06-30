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
select
	o_year,
	sum(peru_volumn) / sum(volume) as mkt_share
from
	(
		select
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			case
				when n2.n_name = 'PERU' then l_extendedprice * (1 - l_discount)
				else 0
			end as peru_volumn
		from
	    	tpch.lineitem
		    inner join tpch.part on l_partkey = p_partkey
		    inner join tpch.supplier on l_suppkey = s_suppkey
			inner join tpch.orders on l_orderkey = o_orderkey
			inner join tpch.customer on o_custkey = c_custkey
		    inner join tpch.nation n1 on c_nationkey = n1.n_nationkey
		    inner join tpch.nation n2 on s_nationkey = n2.n_nationkey
		    inner join tpch.region on n1.n_regionkey = r_regionkey
		where
			r_name = 'AMERICA'
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY BURNISHED NICKEL'
	) as all_nations
group by
	o_year
order by
	o_year