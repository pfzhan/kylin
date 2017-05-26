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

package io.kyligence.kap.storage.parquet.adhoc.util;

import org.junit.Test;

import junit.framework.TestCase;

public class SqlConvertUtilTest extends TestCase {
    @Test
    public void testSringReplace() {
        String originString = "select count(*) as cnt from test_kylin_fact where char_length(lstg_format_name) < 10";
        String replacedString = SqlConvertUtil.replaceString(originString, "char_length", "length");
        assertEquals(replacedString, "select count(*) as cnt from test_kylin_fact where length(lstg_format_name) < 10");
    }

    @Test
    public void testExtractReplace() {
        String originString = "ignore EXTRACT(YEAR FROM KYLIN_CAL_DT.CAL_DT) ignore";
        String replacedString = SqlConvertUtil.extractReplace(originString);
        assertEquals(replacedString, "ignore YEAR(KYLIN_CAL_DT.CAL_DT) ignore");
    }

    @Test
    public void testCastReplace() {
        String originString = "ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS INTEGER)) ignore";
        String replacedString = SqlConvertUtil.castRepalce(originString);
        assertEquals(replacedString, "ignore EXTRACT(YEAR FROM CAST(KYLIN_CAL_DT.CAL_DT AS int)) ignore");
    }

    @Test
    public void testSubqueryReplace() {
        String originString = "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) group by seller_id,lstg_format_name";
        String replacedString = SqlConvertUtil.subqueryRepalce(originString);
        assertEquals(replacedString,
                "select seller_id,lstg_format_name,sum(price) from (select * from test_kylin_fact where (lstg_format_name='FP-GTC') limit 20) as alias group by seller_id,lstg_format_name");
    }

    @Test
    public void testConcatReplace() {
        String originString = "select count(*) as cnt from test_kylin_fact where lstg_format_name||'a'='ABINa'";
        String replacedString = SqlConvertUtil.concatReplace(originString);
        assertEquals(replacedString,
                "select count(*) as cnt from test_kylin_fact where concat(lstg_format_name,'a')='ABINa'");
    }

}
