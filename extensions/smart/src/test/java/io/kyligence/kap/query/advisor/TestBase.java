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
package io.kyligence.kap.query.advisor;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Before;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.Utils;

public class TestBase {
    String metaDir = "src/test/resources/smart/tpch/meta";
    protected KylinConfig kylinConfig = getKylinConfig(metaDir);
    protected SQLResult sqlResult;
    protected Collection<OLAPContext> olapContexts;

    @After
    public void cleanup() throws IOException {

    }

    @Before
    public void setup() throws IOException {

    }

    protected DataModelDesc getDataModelDesc(String modelName) {
        return DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);
    }

    protected CubeDesc getCubeDesc(String cubeName) {
        return CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeName);
    }

    private KylinConfig getKylinConfig(String metaDir) {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        Utils.exposeAllTableAndColumn(kylinConfig);
        return kylinConfig;
    }

    protected void setModelQueryResult(String modelName, String sql) {
        String[] sqls = new String[] { sql };
        DataModelDesc dataModelDesc = getDataModelDesc(modelName);
        AbstractQueryRunner queryRunner = QueryRunnerFactory.createForModelSQLValid(kylinConfig, sqls, 1,
                dataModelDesc);
        setQueryResult(0, queryRunner);
    }

    protected void setCubeQueryResult(String cubeName, String sql) {
        CubeDesc cubeDesc = getCubeDesc(cubeName);
        String[] sqls = new String[] { sql };
        AbstractQueryRunner queryRunner = QueryRunnerFactory.createForCubeSQLValid(kylinConfig, sqls, 1, cubeDesc);
        setQueryResult(0, queryRunner);
    }

    private void setQueryResult(int idx, AbstractQueryRunner queryRunner) {
        try {
            queryRunner.execute();
        } catch (Exception e) {
            System.out.println("batch validate sql error");
        }
        List<SQLResult> queryResults = queryRunner.getQueryResults();
        List<Collection<OLAPContext>> olapContexts = queryRunner.getAllOLAPContexts();
        this.sqlResult = queryResults.get(idx);
        this.olapContexts = olapContexts.get(idx);
    }
}
