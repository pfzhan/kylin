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

with tmp1 as (
    select p_partkey from tpch.part where p_name like 'forest%'
),
tmp2 as (
    select s_name, s_address, s_suppkey
    from tpch.supplier, tpch.nation
    where s_nationkey = n_nationkey
    and n_name = 'CANADA'
),
tmp3 as (
    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
    from tpch.lineitem, tmp2
    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
    and l_suppkey = s_suppkey 
    group by l_partkey, l_suppkey
),
tmp4 as (
    select ps_partkey, ps_suppkey, ps_availqty
    from tpch.partsupp 
    where ps_partkey IN (select p_partkey from tmp1)
),
tmp5 as (
select
    ps_suppkey
from
    tmp4, tmp3
where
    ps_partkey = l_partkey
    and ps_suppkey = l_suppkey
    and ps_availqty > sum_quantity
)
select
    s_name,
    s_address
from
    tpch.supplier
where
    s_suppkey IN (select ps_suppkey from tmp5)
order by s_name;
