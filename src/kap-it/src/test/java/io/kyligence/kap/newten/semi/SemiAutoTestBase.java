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

package io.kyligence.kap.newten.semi;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.newten.SuggestTestBase;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.util.AccelerationContextUtil;
import io.kyligence.kap.util.RecAndQueryCompareUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SemiAutoTestBase extends SuggestTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        dataflowManager.listAllDataflows().stream().map(RootPersistentEntity::getId)
                .forEach(dataflowManager::dropDataflow);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        indexPlanManager.listAllIndexPlans().forEach(indexPlanManager::dropIndexPlan);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelManager.listAllModelIds().forEach(modelManager::dropModel);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected SmartMaster proposeWithSmartMaster(String project, List<TestScenario> testScenarios) throws IOException {
        List<String> sqlList = collectQueries(testScenarios);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));
        String[] sqls = sqlList.toArray(new String[0]);
        AbstractContext context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(), project,
                sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        return smartMaster;
    }

    protected Map<String, ExecAndComp.CompareEntity> collectCompareEntity(SmartMaster smartMaster) {
        Map<String, ExecAndComp.CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new ExecAndComp.CompareEntity());
            final ExecAndComp.CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
            entity.setLevel(RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY);
        });
        return map;
    }

    protected void buildAndCompare(TestScenario... testScenarios) throws Exception {
        try {
            buildAllModels(kylinConfig, getProject());
            compare(testScenarios);
        } finally {
            FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        }
    }

    protected void compare(TestScenario... testScenarios) {
        Arrays.stream(testScenarios)
                .forEach(testScenario -> compare(testScenario, testScenario.getQueries()));
    }

    private void compare(TestScenario testScenario, List<Pair<String, String>> validQueries) {
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        if (testScenario.isLimit()) {
            ExecAndComp.execLimitAndValidateNew(validQueries, getProject(), JoinType.DEFAULT.name(), null);
        } else if (testScenario.isDynamicSql()) {
            ExecAndComp.execAndCompareDynamic(validQueries, getProject(), testScenario.getCompareLevel(),
                    testScenario.getJoinType().name(), null);
        } else {
            ExecAndComp.execAndCompare(validQueries, getProject(), testScenario.getCompareLevel(),
                    testScenario.getJoinType().name());
        }
    }
}
