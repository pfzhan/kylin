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
package io.kyligence.kap.source.jdbc;

import java.sql.SQLException;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NSparkCubingEngine;

public class JdbcSourceInputTest extends JdbcTestBase {
    private static SparkSession ss;

    @BeforeClass
    public static void setUp() throws SQLException {
        JdbcTestBase.setUp();
        ss = SparkSession.builder().master("local").getOrCreate();
    }

    @Test
    public void testGetSourceData() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        TableDesc tableDesc = tableMgr.getTableDesc("SSB.P_LINEORDER");
        ISource source = SourceFactory.getSource(new ISourceAware() {
            @Override
            public int getSourceType() {
                return ISourceAware.ID_JDBC;
            }

            @Override
            public KylinConfig getConfig() {
                return getTestConfig();
            }
        });
        NSparkCubingEngine.NSparkCubingSource cubingSource = source
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingSource.class);
        Dataset<Row> sourceData = cubingSource.getSourceData(tableDesc, ss, Maps.newHashMap());
        System.out.println(sourceData.schema());
    }
}
