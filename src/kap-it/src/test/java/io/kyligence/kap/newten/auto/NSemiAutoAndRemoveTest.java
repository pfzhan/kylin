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

package io.kyligence.kap.newten.auto;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.tool.garbage.IndexCleaner;
import io.kyligence.kap.utils.RecAndQueryCompareUtil;
import lombok.val;

public class NSemiAutoAndRemoveTest extends SemiAutoTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NSemiAutoAndRemoveTest.class);

    @Before
    public void setup() throws Exception {
        super.setup();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "default";
    }

    @Test
    public void testAllQueries() throws Exception {
        overwriteSystemProp("kap.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kap.smart.conf.auto-modeling.non-equi-join.enabled", "TRUE");
        val testScenarios = new TestScenario[] { new TestScenario(NExecAndComp.CompareLevel.SAME, "semi_auto/remove") };
        prepareModels(getProject(), testScenarios);
        executeTestScenario(testScenarios);//
    }

    private Set<Pair<String, Long>> removeLayouts() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        Set<Pair<String, Long>> removeLayouts = indexPlanManager.listAllIndexPlans().stream()
                .flatMap(indexPlan -> Lists
                        .newArrayList(new Pair<>(indexPlan.getUuid(), 1L), new Pair<>(indexPlan.getUuid(), 10001L))
                        .stream())
                .collect(Collectors.toSet());
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        val indexplanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        removeLayouts.stream().collect(Collectors.groupingBy(Pair::getFirst)).forEach((modelId, value) -> {
            val layouts = value.stream().map(Pair::getSecond).collect(Collectors.toSet());
            val layoutIdMap = indexplanManager.getIndexPlan(modelId).getAllLayouts().stream()
                    .collect(Collectors.toMap(LayoutEntity::getId, l -> l));
            layouts.forEach(id -> {
                dataflowManager.updateDataflow(modelId, copyForWrite -> {
                    copyForWrite.getLayoutHitCount().remove(id);
                });
                val layout = layoutIdMap.get(id);
                if (layout.isManual() && layout.getId() < IndexEntity.TABLE_INDEX_START_ID) {
                    indexplanManager.updateIndexPlan(modelId,
                            copyForWrite -> copyForWrite.getRuleBasedIndex().setLastModifiedTime(0L));
                } else {
                    indexplanManager.updateIndexPlan(modelId, copyForWrite -> {
                        val indexIdMap = copyForWrite.getIndexes().stream()
                                .collect(Collectors.toMap(IndexEntity::getId, i -> i));
                        val index = indexIdMap.get((id / IndexEntity.INDEX_ID_STEP) * IndexEntity.INDEX_ID_STEP);
                        index.getLayouts().stream().filter(l -> l.getId() == id).forEach(l -> l.setUpdateTime(0L));
                    });
                }
            });
        });
        collectLogMsg(removeLayouts);
        new IndexCleaner().cleanup(getProject());
        return removeLayouts;
    }

    private void collectLogMsg(Set<Pair<String, Long>> removeLayouts) {
        removeLayouts.forEach(p -> {
            val id = p.getFirst();
            val layoutId = p.getSecond();
            val indexPlan = NIndexPlanManager.getInstance(getTestConfig(), getProject()).getIndexPlan(id);
            val layout = indexPlan.getCuboidLayout(layoutId);
            logger.info("remove index {} layout {} col {} shard by {} sort by {}", id, layoutId, layout.getColOrder(),
                    layout.getShardByColumns(), layout.getSortByColumns());
        });
    }

    protected void buildAndCompare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            TestScenario... testScenarios) throws Exception {
        try {
            buildAllCubes(kylinConfig, getProject());
            compare(compareMap, testScenarios);
            val removeLayouts = removeLayouts();
            verifyAll();
            compare(compareMap, removeLayouts, testScenarios);
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }
    }

    private void compare(Map<String, RecAndQueryCompareUtil.CompareEntity> compareMap,
            Set<Pair<String, Long>> removeLayouts, TestScenario... testScenarios) {
        Arrays.stream(testScenarios).forEach(testScenario -> {
            val validQueries = testScenario.queries.stream()
                    .filter(p -> Sets.intersection(removeLayouts,
                            compareMap.get(p.getValue()).getAccelerateInfo().getRelatedLayouts().stream()
                                    .map(layout -> new Pair<>(layout.getModelId(), layout.getLayoutId()))
                                    .collect(Collectors.toSet()))
                            .isEmpty())
                    .collect(Collectors.toList());
            val invalidQueries = Lists.newArrayList(
                    Sets.difference(Sets.newHashSet(testScenario.queries), Sets.newHashSet(validQueries)));
            compare(compareMap, testScenario, validQueries, invalidQueries);
        });
    }

}
