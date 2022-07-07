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
package io.kyligence.kap.engine.spark.source;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;


public class SparkSqlUtilTest extends NLocalWithSparkSessionTest {

    @Test
    public void getViewOrignalTables() throws Exception {
        val tableName = "tableX";
        try {
            //get table tableX
            ss.sql("CREATE TABLE IF NOT EXISTS tableX (a int, b int) using csv");

            //get view v_tableX
            ss.sql("CREATE VIEW IF NOT EXISTS v_tableX as select * from " + tableName);

            //get nested view v_tableX_nested
            ss.sql("CREATE VIEW IF NOT EXISTS v_tableX_nested as select * from v_tableX");

            Assert.assertEquals(tableName, SparkSqlUtil.getViewOrignalTables("v_tableX", ss).iterator().next());

            Assert.assertEquals(tableName, SparkSqlUtil.getViewOrignalTables("v_tableX_nested", ss).iterator().next());
        } finally {
            ss.sql("DROP VIEW IF EXISTS v_tableX_nested");
            ss.sql("DROP VIEW IF EXISTS v_tableX");
            ss.sql("DROP TABLE IF EXISTS " + tableName);
        }
    }
}