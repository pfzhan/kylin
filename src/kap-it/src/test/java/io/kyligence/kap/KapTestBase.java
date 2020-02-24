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

package io.kyligence.kap;

import java.sql.DriverManager;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SandboxMetadataTestCase;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.source.jdbc.H2Database;

public class KapTestBase extends KylinTestBase {

    protected static void setupAll() throws Exception {
        initQueryEngine();
        //setup env
        SandboxMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //setup cube conn
        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
//        cubeConnection = QueryConnection.getConnection(project);

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++) + ";CACHE_SIZE=32072", "sa",
                "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config, project);
        h2DB.loadAllTables();
    }

    public static void initQueryEngine() {
        if (System.getProperty("sparder.enabled") != null && System.getProperty("sparder.enabled").equals("true")) {
            System.setProperty("kap.query.engine.sparder-enabled", "true");
            System.setProperty("kylin.query.schema-factory", "io.kyligence.kap.query.schema.KapSchemaFactory");
        } else {
            System.setProperty("kap.query.engine.sparder-enabled", "false");
            System.setProperty("kylin.query.schema-factory", "org.apache.kylin.query.schema.OLAPSchemaFactory");
        }

        if (System.getProperty("spark.local") != null && System.getProperty("spark.local").equalsIgnoreCase("true")) {
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        }
    }
}
