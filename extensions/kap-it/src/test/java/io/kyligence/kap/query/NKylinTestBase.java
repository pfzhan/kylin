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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

/**
 */
public class NKylinTestBase extends KylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NKylinTestBase.class);

    protected static void setupAll() throws Exception {
        //setup env
        NLocalFileMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.store-factory", "io.kyligence.kap.common.persistence.KapMetaStoreFactory");

        //setup cube conn
        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        cubeConnection = QueryConnection.getConnection(project);

        //setup h2
        //        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++) + ";CACHE_SIZE=32072", "sa",
        //                "");
        // Load H2 Tables (inner join)
        //        H2Database h2DB = new H2Database(h2Connection, config, project);
        //        h2DB.loadAllTables();
    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        NLocalFileMetadataTestCase.staticCleanupTestMetadata();
        RemoveBlackoutRealizationsRule.blackList.clear();

    }

}
