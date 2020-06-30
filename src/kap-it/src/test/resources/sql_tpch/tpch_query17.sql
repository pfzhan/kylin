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
with q17_part as (
  select p_partkey from tpch.part where  
  p_brand = 'Brand#23'
  and p_container = 'MED BOX'
),
q17_avg as (
  select l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity
  from tpch.lineitem 
  where l_partkey IN (select p_partkey from q17_part)
  group by l_partkey
),
q17_price as (
  select
  l_quantity,
  l_partkey,
  l_extendedprice
  from
  tpch.lineitem
  where
  l_partkey IN (select p_partkey from q17_part)
)
select cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
from q17_avg, q17_price
where 
t_partkey = l_partkey and l_quantity < t_avg_quantity;
