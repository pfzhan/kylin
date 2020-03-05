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

package io.kyligence.kap.tool.garbage;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.GarbageLayoutType;
import io.kyligence.kap.metadata.cube.optimization.IndexOptimizerFactory;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexCleaner implements MetadataCleaner {

    public void cleanup(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val projectInstance = NProjectManager.getInstance(config).getProject(project);

        for (val model : dataflowManager.listUnderliningDataModels()) {
            val dataflow = dataflowManager.getDataflow(model.getId()).copy();
            Map<Long, GarbageLayoutType> garbageLayouts = IndexOptimizerFactory.getOptimizer(dataflow, true)
                    .getGarbageLayoutMap(dataflow);

            if (MapUtils.isEmpty(garbageLayouts)) {
                continue;
            }

            if (projectInstance.isSemiAutoMode()) {
                // cannot update layout hit count when transfer to recommendation,
                // but need to update layout hit count when applying recommendation
                // refer to LayoutRecommendationItem.apply()
                transferToRecommendation(model.getUuid(), project, garbageLayouts);
            }

            if (projectInstance.isSmartMode() || projectInstance.isExpertMode()) {
                // update layout hit count
                dataflowManager.updateDataflow(dataflow.getUuid(),
                        copyForWrite -> copyForWrite.setLayoutHitCount(dataflow.getLayoutHitCount()));
                cleanupIsolatedIndex(project, model.getId(), garbageLayouts.keySet());
            }
        }
    }

    private void transferToRecommendation(String modelId, String project, Map<Long, GarbageLayoutType> garbageLayouts) {
        OptimizeRecommendationManager recMgr = OptimizeRecommendationManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project);
        recMgr.removeLayouts(modelId, garbageLayouts);
    }

    private void cleanupIsolatedIndex(String project, String modelId, Set<Long> garbageLayouts) {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(),
                copyForWrite -> copyForWrite.removeLayouts(garbageLayouts, LayoutEntity::equals, true, false));
    }
}
