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
        Assert.assertEquals("select sum(price) as GMV group by \n"
                + "lstg_format_name,cal_dt,slr_segment_cd grouping sets((lstg_format_name, cal_dt, slr_segment_cd), (cal_dt, slr_segment_cd), (lstg_format_name, slr_segment_cd));\n",
                replacedString);
    }

    @Test
    public void testCastVariantSubstringGrammar() {
        HivePushDownConverter converter = new HivePushDownConverter();
        String originString = "select substring( lstg_format_name   from   1  for   4 ) from test_kylin_fact limit 10;";
        String replacedString = converter.convert(originString, "", "", false);
        Assert.assertEquals("select substring(lstg_format_name, 1, 4) from test_kylin_fact limit 10;", replacedString);

        originString = "select distinct " //
                + "substring (\"ZB_POLICY_T_VIEW\".\"DIMENSION1\" " //
                + "\nfrom position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") + 3 " //
                + "\nfor (position ('|2|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\") - position ('|1|' in \"ZB_POLICY_T_VIEW\".\"DIMENSION1\")) - 3"
                + ") as \"memberUniqueName\"  " //
                + "from \"FRPDB0322\".\"ZB_POLICY_T_VIEW\" \"ZB_POLICY_T_VIEW\" limit10;";
        replacedString = converter.convert(originString, "", "", false);
        Assert.assertEquals("select distinct " //
                + "substring(`ZB_POLICY_T_VIEW`.`DIMENSION1`, " //
                + "position ('|1|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`) + 3, " //
                + "(position ('|2|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`) - position ('|1|' in `ZB_POLICY_T_VIEW`.`DIMENSION1`)) - 3"
                + ") as `memberUniqueName`  " //
                + "from `FRPDB0322`.`ZB_POLICY_T_VIEW` `ZB_POLICY_T_VIEW` limit10;", //
                replacedString);
    }

}
