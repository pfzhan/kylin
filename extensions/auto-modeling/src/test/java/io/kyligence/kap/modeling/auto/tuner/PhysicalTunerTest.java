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

package io.kyligence.kap.modeling.auto.tuner;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.modeling.auto.ModelingContext;
import io.kyligence.kap.modeling.auto.ModelingContextBuilder;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

public class PhysicalTunerTest extends LocalFileMetadataTestCase {
    private static final String CUBE_AUTO_DESC = "test_kylin_cube_auto";
    private static MetadataManager metadataManager;
    private static ModelStatsManager modelStatsManager;

    private CubeDesc getOneCubeDesc() {
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc(CUBE_AUTO_DESC);
        cubeDesc = CubeDesc.getCopyOf(cubeDesc);

        for (AggregationGroup aggregationGroup : cubeDesc.getAggregationGroups()) {
            SelectRule rule = new SelectRule();
            rule.hierarchy_dims = new String[0][0];
            rule.joint_dims = new String[0][0];
            rule.mandatory_dims = new String[0];
            aggregationGroup.setSelectRule(rule);
        }
        cubeDesc.init(getTestConfig());
        return cubeDesc;
    }

    private ModelingContext buildContext(DataModelDesc modelDesc) throws IOException {
        ModelingContextBuilder builder = new ModelingContextBuilder();
        TableDesc t1 = metadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TableDesc t2 = metadataManager.getTableDesc("DEFAULT.TEST_CATEGORY_GROUPINGS");
        TableDesc t3 = metadataManager.getTableDesc("EDW.TEST_CAL_DT");
        TableDesc t4 = metadataManager.getTableDesc("EDW.TEST_SELLER_TYPE_DIM");
        TableDesc t5 = metadataManager.getTableDesc("EDW.TEST_SITES");
        TableExtDesc te1 = metadataManager.getTableExt("DEFAULT.TEST_KYLIN_FACT");
        TableExtDesc te2 = metadataManager.getTableExt("DEFAULT.TEST_CATEGORY_GROUPINGS");
        TableExtDesc te3 = metadataManager.getTableExt("EDW.TEST_CAL_DT");
        TableExtDesc te4 = metadataManager.getTableExt("EDW.TEST_SELLER_TYPE_DIM");
        TableExtDesc te5 = metadataManager.getTableExt("EDW.TEST_SITES");
        ModelStats ms = modelStatsManager.getModelStats(modelDesc.getName());

        builder.addTable(t1, te1);
        builder.addTable(t2, te2);
        builder.addTable(t3, te3);
        builder.addTable(t4, te4);
        builder.addTable(t5, te5);
        builder.setModelDesc(modelDesc);
        builder.setModelStats(ms);

        return builder.build();
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        KylinConfig config = getTestConfig();
        metadataManager = MetadataManager.getInstance(config);
        modelStatsManager = ModelStatsManager.getInstance(config);
    }

    @After
    public void cleanup() throws IOException {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        CubeDesc cubeDesc = getOneCubeDesc();

        ModelingContext context = buildContext(cubeDesc.getModel());
        PhysicalTuner tuner = new PhysicalTuner(context);
        tuner.optimize(cubeDesc);

        System.out.println("");
        System.out.println(JsonUtil.writeValueAsIndentString(cubeDesc));
    }
}
