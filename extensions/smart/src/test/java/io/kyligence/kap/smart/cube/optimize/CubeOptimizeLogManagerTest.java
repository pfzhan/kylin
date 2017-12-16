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

package io.kyligence.kap.smart.cube.optimize;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.smart.common.MasterFactory;
import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.cube.CubeOptimizeLog;
import io.kyligence.kap.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.smart.query.Utils;

public class CubeOptimizeLogManagerTest extends LocalFileMetadataTestCase {

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Ignore
    @Test
    public void testBasics() throws IOException {
        String cubeName = "ci_left_join_cube";
        String[] sqls = { "select * from table1", "select count(*) from table2", "select sum(column1) from table3",
                "select lstg_format_name, sum(price) from kylin_sales group by lstg_format_name" };

        KylinConfig kylinConfig = Utils.newKylinConfig("src/test/resources/smart/learn_kylin/meta");
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");

        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc("kylin_sales_model");

        CubeMaster master = MasterFactory.createCubeMaster(kylinConfig, modelDesc, sqls);
        CubeOptimizeLogManager manager = CubeOptimizeLogManager.getInstance(kylinConfig);
        manager.removeCubeOptimizeLog(cubeName);
        CubeOptimizeLog cubeOptimizeLog = manager.getCubeOptimizeLog(cubeName);
        cubeOptimizeLog.setSampleSqls(Arrays.asList(sqls));
        cubeOptimizeLog.setQueryStats(master.getContext().getQueryStats());
        manager.saveCubeOptimizeLog(cubeOptimizeLog);

        CubeOptimizeLog newOne = manager.getCubeOptimizeLog(cubeName);
        List<String> sampleSqls = newOne.getSampleSqls();
        assertEquals(4, sampleSqls.size());
        assertEquals(1, cubeOptimizeLog.getQueryStats().getTotalQueries());
    }

}
