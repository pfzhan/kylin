/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

 
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.


 */

package org.apache.kylin.source.adhocquery;

import org.junit.Assert;
import org.junit.Test;

public class HivePushDownConverterTest {
    @Test
    public void testStringReplace() {
        String originString = "select count(*) as cnt from test_kylin_fact where char_length(lstg_format_name) < 10";
        String replacedString = HivePushDownConverter.replaceString(originString, "char_length", "length");
        Assert.assertEquals("select count(*) as cnt from test_kylin_fact where length(lstg_format_name) < 10",
                replacedString);
    }

    @Test
    public void testExtractReplace() {
        String originString = "ignore EXTRACT(YEAR FROM KYLIN_CAL_DT.CAL_DT) ignore";
        String replacedString = HivePushDownConverter.extractReplace(originString);
        Assert.assertEquals("ignore YEAR(KYLIN_CAL_DT.CAL_DT) ignore", replacedString);
    }

    @Test
    public void testCastReplace() {
        String originString = "ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS INTEGER)) ignore";
        String replacedString = HivePushDownConverter.castReplace(originString);
        Assert.assertEquals("ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS int)) ignore", replacedString);
    }

    @Test
    public void testSubqueryReplace1() {
        String originString = "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) group by seller_id,lstg_format_name";
        String replacedString = HivePushDownConverter.subqueryReplace(originString);
        Assert.assertEquals(
                "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) as alias group by seller_id,lstg_format_name",
                replacedString);
    }

    @Test
    public void testSubqueryReplace2() {
        String originString = "select count(*) from ( select test_kylin_fact.lstg_format_name from test_kylin_fact where test_kylin_fact.lstg_format_name='FP-GTC' group by test_kylin_fact.lstg_format_name ) t ";
        String replacedString = HivePushDownConverter.subqueryReplace(originString);
        Assert.assertEquals(originString, replacedString);
    }

    @Test
    public void testSubqueryReplace3() {
        String originString = "select fact.lstg_format_name from (select * from test_kylin_fact where cal_dt > date'2010-01-01' ) as fact group by fact.lstg_format_name order by CASE WHEN fact.lstg_format_name IS NULL THEN 'sdf' ELSE fact.lstg_format_name END ";
        String replacedString = HivePushDownConverter.subqueryReplace(originString);
        Assert.assertEquals(originString, replacedString);
    }

    @Test
    public void testSubqueryReplace4() {
        String originString = "select t.TRANS_ID from (\n"
                + "    select * from test_kylin_fact s inner join TEST_ACCOUNT a \n"
                + "        on s.BUYER_ID = a.ACCOUNT_ID inner join TEST_COUNTRY c on c.COUNTRY = a.ACCOUNT_COUNTRY\n"
                + "    )t\n" + "LIMIT 50000";
        String replacedString = HivePushDownConverter.subqueryReplace(originString);
        Assert.assertEquals(originString, replacedString);
    }

    @Test
    public void testConcatReplace1() {
        String originString = "select count(*) as cnt from test_kylin_fact where 'abc' ||lstg_format_name|| 'a'|| 'b' ||'c'='abcABINabc'";
        String replacedString = HivePushDownConverter.concatReplace(originString);
        Assert.assertEquals(
                "select count(*) as cnt from test_kylin_fact where concat('abc',lstg_format_name,'a','b','c')='abcABINabc'",
                replacedString);
    }

    @Test
    public void testConcatReplace2() {
        String originString = "select count(*) as cnt from test_kylin_fact where lstg_format_name = 'A' || 'B' || 'IN' and lstg_format_name = 'AB' || 'IN'";
        String replacedString = HivePushDownConverter.concatReplace(originString);
        Assert.assertEquals(
                "select count(*) as cnt from test_kylin_fact where lstg_format_name = concat('A','B','IN') and lstg_format_name = concat('AB','IN')",
                replacedString);
    }

    @Test
    public void testConcatReplace3() {
        String originString = "select count(*) as cnt from test_kylin_fact where ('abc') ||lstg_format_name|| (('a'|| 'b') ||'c')='abcABINabc'";
        String replacedString = HivePushDownConverter.concatReplace(originString);
        Assert.assertEquals(
                "select count(*) as cnt from test_kylin_fact where concat('abc',lstg_format_name,'a','b','c')='abcABINabc'",
                replacedString);
    }

    @Test
    public void testAddLimit() {
        String originString = "select t.TRANS_ID from (\n"
                + "    select * from test_kylin_fact s inner join TEST_ACCOUNT a \n"
                + "        on s.BUYER_ID = a.ACCOUNT_ID inner join TEST_COUNTRY c on c.COUNTRY = a.ACCOUNT_COUNTRY\n"
                + "     limit 10000)t\n";
        String replacedString = HivePushDownConverter.addLimit(originString);
        Assert.assertEquals(originString.concat(" limit 1"), replacedString);
    }

    @Test
    public void testGroupingSets() {
        String originString = "select sum(price) as GMV group by \n"
                + "grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd));\n";
        String replacedString = HivePushDownConverter.groupingSetsReplace(originString);
        Assert.assertEquals(
                "select sum(price) as GMV group by \n"
                        + "lstg_format_name,cal_dt,slr_segment_cd grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd));\n",
                replacedString);
    }

}
