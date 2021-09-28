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

package io.kyligence.kap.secondstorage.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SecondStorageSqlUtilsTest {

    @Test
    public void test() {
        String createTableSql = "CREATE TABLE ke_metadata_p_test_01\n"
                + ".cfb0ae66_7d59_4eb7_bb0b_bd0ce57069ed_20000000001_temp\n"
                + "(`c0` Nullable(Int64),`c1` Nullable(Int32),`c7` Nullable(Int32),`c10` Date,`c15` Nullable(Int64),`c16` Nullable(Int32)) \n"
                + "ENGINE = MergeTree() \n"
                + "PARTITION BY `c10` ORDER BY tuple()";
        String createTableSqlExpected = "CREATE TABLE IF NOT EXISTS ke_metadata_p_test_01\n"
                + ".cfb0ae66_7d59_4eb7_bb0b_bd0ce57069ed_20000000001_temp\n"
                + "(`c0` Nullable(Int64),`c1` Nullable(Int32),`c7` Nullable(Int32),`c10` Date,`c15` Nullable(Int64),`c16` Nullable(Int32)) \n"
                + "ENGINE = MergeTree() \n"
                + "PARTITION BY `c10` ORDER BY tuple()";

        String createTableSql2 = SecondStorageSqlUtils.addIfNotExists(createTableSql, "TABLE");
        Assertions.assertEquals(createTableSql2, createTableSqlExpected);

        String createDatabaseSql = "CREATE DATABASE ke_metadata_p_test_01\n"
                + "ENGINE = Atomic";
        String createDatabaseSqlExcepted = "CREATE DATABASE IF NOT EXISTS ke_metadata_p_test_01\n"
                + "ENGINE = Atomic";

        String createDatabaseSql2 = SecondStorageSqlUtils.addIfNotExists(createDatabaseSql, "DATABASE");
        Assertions.assertEquals(createDatabaseSql2, createDatabaseSqlExcepted);
    }
}
