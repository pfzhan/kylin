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

package io.kyligence.kap.modeling.smart;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.query.mockup.Utils;

@Ignore("Ignore because this is only used for demo.")
public class ModelingMasterTest {
    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws IOException {
        testInternal("src/test/resources/learn_kylin/meta", "kylin_sales_model", "src/test/resources/learn_kylin/sql");
    }

    @Test
    public void testE2E_SSB() throws IOException {
        testInternal("src/test/resources/ssb/meta", "ssb", "src/test/resources/ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws IOException {
        testInternal("src/test/resources/tpch/meta", "lineitem_model", "src/test/resources/tpch/sql_lineitem");
    }

    @Test
    public void testE2E_Airline() throws IOException {
        testInternal("src/test/resources/airline/meta", "airline_model", null);
    }

    private void testInternal(String metaDir, String modelName, String sqlDir) throws IOException {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        DataModelDesc modelDesc = MetadataManager.getInstance(kylinConfig).getDataModelDesc(modelName);

        File[] sqlFiles = new File[0];
        if (sqlDir != null) {
            File sqlDirF = new File(sqlDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(sqlDir).listFiles();
            }
        }

        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }

        ModelingMaster master = ModelingMasterFactory.create(kylinConfig, modelDesc, sqls);

        // get initial cube
        CubeDesc initCube = master.proposeInitialCube();

        // get dimension and measure
        CubeDesc dimMeasCube = master.proposeDerivedDimensions(initCube);

        // get rowkey
        CubeDesc rowkeyCube = master.proposeRowkey(dimMeasCube);

        // get aggr groups
        CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);

        // get override cube
        CubeDesc configOverrideCube = master.proposeConfigOverride(aggGroupCube);

        System.out.println(JsonUtil.writeValueAsIndentString(configOverrideCube));
    }
}
