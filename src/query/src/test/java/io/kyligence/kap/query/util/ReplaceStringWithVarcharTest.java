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
package io.kyligence.kap.query.util;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplaceStringWithVarcharTest extends NLocalFileMetadataTestCase {
    private static final ReplaceStringWithVarchar replaceStringWithVarchar = new ReplaceStringWithVarchar();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBase() {
        // test cases ref: https://dev.mysql.com/doc/refman/8.0/en/select.html
        String[] actuals = new String[] {
                // select, from, where, like, group by, having, order by, subclause
                "Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)>'2012-01-01' order by cast(D1.c1 as STRING)",
                // over(window)
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS STRING)) from TEST_KYLIN_FACT",
                // case when else
                "select case cast(CAL_DT as STRING) when cast('2012-01-13' as STRING) THEN cast('1' as STRING) ELSE cast('null' as STRING) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as STRING)",
                // join
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as STRING)=cast(b.CAL_DT as STRING) limit 100",
                // union
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as STRING)='Auction' limit 100",
                // with
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as STRING) as c2 from (select distinct substring(cast(T33458.CAL_DT as STRING), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as STRING) like cast('2012-01%' as STRING) group by cast(D1.c1 as STRING) having cast(D1.c1 as STRING)>'2012-01-01' order by cast(D1.c1 as STRING)) \n"
                        + "SELECT * from t1\n"
                        + "UNION ALL\n"
                        + "SELECT * from t1"
        };
        String[] expecteds = new String[] {
                "Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct substring(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)>'2012-01-01' order by cast(D1.c1 as VARCHAR)",
                "select TRANS_ID, CAL_DT, LSTG_FORMAT_NAME, MAX(CAL_DT) over (PARTITION BY CAST(LSTG_FORMAT_NAME AS VARCHAR)) from TEST_KYLIN_FACT",
                "select case cast(CAL_DT as VARCHAR) when cast('2012-01-13' as VARCHAR) THEN cast('1' as VARCHAR) ELSE cast('null' as VARCHAR) END AS STRING, CAL_DT from TEST_KYLIN_FACT order by cast(CAL_DT as VARCHAR)",
                "select * from TEST_KYLIN_FACT a left join EDW.TEST_CAL_DT b on cast(a.CAL_DT as VARCHAR)=cast(b.CAL_DT as VARCHAR) limit 100",
                "select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR)='FP-GTC' union select * from TEST_KYLIN_FACT where cast(LSTG_FORMAT_NAME as VARCHAR)='Auction' limit 100",
                "WITH t1 AS (Select 0 as STRING, cast(D1.c1 as VARCHAR) as c2 from (select distinct substring(cast(T33458.CAL_DT as VARCHAR), 1, 30) as c1 from TEST_KYLIN_FACT T33458) D1 where cast(D1.c1 as VARCHAR) like cast('2012-01%' as VARCHAR) group by cast(D1.c1 as VARCHAR) having cast(D1.c1 as VARCHAR)>'2012-01-01' order by cast(D1.c1 as VARCHAR)) \n"
                        + "SELECT * from t1\n"
                        + "UNION ALL\n"
                        + "SELECT * from t1"
        };
        for (int i=0; i<actuals.length; ++i) {
            String transformedActual = replaceStringWithVarchar.transform(actuals[i], "", "");
            Assert.assertEquals(expecteds[i], transformedActual);
            System.out.println("\nTRANSFORM SUCCEED\nBEFORE:\n" + actuals[i] + "\nAFTER:\n" + expecteds[i]);
        }
    }
}
