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

SELECT 
 test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 ,test_category_groupings.categ_lvl2_name 
 ,test_category_groupings.categ_lvl3_name 
 ,test_kylin_fact.lstg_format_name 
 ,test_sites.site_name 
 ,test_seller_type_dim.seller_type_desc 
 ,sum(test_kylin_fact.price) as GMV 
 , count(*) as TRANS_CNT 
 FROM test_kylin_fact 
 inner JOIN edw.test_cal_dt as test_cal_dt 
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt 
 inner JOIN test_category_groupings 
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id 
 inner JOIN edw.test_sites as test_sites 
 ON test_kylin_fact.lstg_site_id = test_sites.site_id 
 inner JOIN edw.test_seller_type_dim as test_seller_type_dim 
 ON test_kylin_fact.slr_segment_cd = test_seller_type_dim.seller_type_cd 
 group by test_cal_dt.week_beg_dt 
 ,test_category_groupings.meta_categ_name 
 ,test_category_groupings.categ_lvl2_name 
 ,test_category_groupings.categ_lvl3_name 
 ,test_kylin_fact.lstg_format_name 
 ,test_sites.site_name 
 ,test_seller_type_dim.seller_type_desc 
 
