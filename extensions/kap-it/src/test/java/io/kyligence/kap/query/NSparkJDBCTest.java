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

package io.kyligence.kap.query;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.NLocalSparkWithMetaTest;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;

public class NSparkJDBCTest extends NLocalSparkWithMetaTest {
    @Test
    public void testSparkJDBC() throws SQLException {
        Assert.assertNotNull(QueryConnection.getConnection(ProjectInstance.DEFAULT_PROJECT_NAME));
        System.setProperty("kap.storage.columnar.hdfs-dir",
                NLocalFileMetadataTestCase.tempMetadataDirectory.getAbsolutePath() + "/parquet/");

        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(ProjectInstance.DEFAULT_PROJECT_NAME, getTestConfig());
        Properties prop = new Properties();
        prop.put("model", olapTmp.getAbsolutePath());
        String formatSql = NSparkCubingUtil.formatSQL("select count(*) as CNT from test_kylin_fact");
        Dataset<Row> ret = ss.read().jdbc("jdbc:calcite:", formatSql, prop);
        ret.show(10);
    }
}
