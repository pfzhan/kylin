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

package io.kyligence.kap.query.validate;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class UserDataTest extends TestBase {

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void smilingmoon() throws IOException {
        validateModel("/Users/dong/Desktop/User_Test_Cases/890cffe8ab3906fd1296c3a0da6bbb72/metadata", "yzl_test_wd",
                "/Users/dong/Desktop/User_Test_Cases/890cffe8ab3906fd1296c3a0da6bbb72/sql.txt");
    }

    @Test
    public void testModel() throws IOException {
        validateModel("src/test/resources/tpch/meta", "lineitem_model", "src/test/resources/tpch/sql_lineitem");
    }

    @Ignore
    @Test
    public void testCube() throws IOException {
        validateCube("src/test/resources/tpch/meta", "customer_cube", "src/test/resources/tpch/sql_lineitem");
    }

    //bad sql
    @Test
    public void testCubeBadSql() throws IOException {
        validateCube("src/test/resources/tpch/meta", "customer_cube", "src/test/resources/tpch/bad_sql");
    }

    @Test
    public void testModelBadSql() throws IOException {
        validateModel("src/test/resources/tpch/meta", "lineitem_model", "src/test/resources/tpch/bad_sql");
    }

    //model
    @Test
    public void testModelJoin() throws IOException {
        validateModel("src/test/resources/tpch/modelJoin", "lineitem_model", "src/test/resources/tpch/modelJoin/sql");
    }
}
