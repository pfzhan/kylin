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

package org.apache.kylin.source.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.source.datagen.ModelDataGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NJdbcTableReaderTest extends NLocalFileMetadataTestCase implements ISourceAware {

    protected KylinConfig config = null;
    protected static Connection h2Connection = null;

    @Before
    public void setup() throws Exception {

        super.createTestMetadata();

        System.setProperty("kylin.source.jdbc.connection-url", "jdbc:h2:mem:db" + "_jdbc_table_reader");
        System.setProperty("kylin.source.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.source.jdbc.user", "sa");
        System.setProperty("kylin.source.jdbc.pass", "");

        config = KylinConfig.getInstanceFromEnv();

        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + "_jdbc_table_reader", "sa", "");

        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        H2Database h2DB = new H2Database(h2Connection, config, project);

        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = mgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        ModelDataGenerator gen = new ModelDataGenerator(model, 10000,
                ResourceStore.getKylinMetaStore(model.getConfig()));
        gen.generate();

        h2DB.loadAllTables();

    }

    @After
    public void after() throws Exception {

        super.cleanupTestMetadata();

        if (h2Connection != null) {
            try {
                h2Connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.clearProperty("kylin.source.jdbc.connection-url");
        System.clearProperty("kylin.source.jdbc.driver");
        System.clearProperty("kylin.source.jdbc.user");
        System.clearProperty("kylin.source.jdbc.pass");

    }

    @Test
    public void test() throws Exception {

        JdbcTableReader reader = new JdbcTableReader("default", "test_kylin_fact");
        int rowNumber = 0;
        while (reader.next()) {
            String[] row = reader.getRow();
            Assert.assertEquals(11, row.length);

            rowNumber++;
        }

        reader.close();
        Assert.assertEquals(10000, rowNumber);

    }

    @Override
    public int getSourceType() {
        return ISourceAware.ID_JDBC;
    }

}
