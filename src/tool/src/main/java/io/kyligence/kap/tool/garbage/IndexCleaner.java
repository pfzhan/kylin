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

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.optimization.GarbageLayoutType;
import io.kyligence.kap.metadata.cube.optimization.IndexOptimizerFactory;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexCleaner extends MetadataCleaner {

    List<String> needUpdateModels = Lists.newArrayList();

    public IndexCleaner(String project) {
        super(project);
    }

    @Override
    public void prepare() {
        log.info("Start to clean index in project {}", project);
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val projectInstance = NProjectManager.getInstance(config).getProject(project);

        if (projectInstance.isExpertMode()) {
            log.info("not semiautomode, can't run index clean");
            return;
        }
        OptRecManagerV2 recManagerV2 = OptRecManagerV2.getInstance(project);
        for (val model : dataflowManager.listUnderliningDataModels()) {
            val dataflow = dataflowManager.getDataflow(model.getId()).copy();
            Map<Long, GarbageLayoutType> garbageLayouts = IndexOptimizerFactory.getOptimizer(dataflow, true)
                    .getGarbageLayoutMap(dataflow);

            if (MapUtils.isEmpty(garbageLayouts)) {
                continue;
            }
            boolean hasNewRecItem = recManagerV2.genRecItemsFromIndexOptimizer(project, model.getUuid(),
                    garbageLayouts);
            if (hasNewRecItem) {
                needUpdateModels.add(model.getId());
            }
        }

        log.info("Clean index in project {} finished", project);
    }

    @Override
    public void cleanup() {
        if (needUpdateModels.isEmpty()) {
            return;
        }
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        needUpdateModels.forEach(modelId -> {
            NDataModel dataModel = mgr.getDataModelDesc(modelId);
            if (dataModel != null && !dataModel.isBroken()) {
                OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
                int newSize = optRecV2.getAdditionalLayoutRefs().size() + optRecV2.getRemovalLayoutRefs().size();
                if (dataModel.getRecommendationsCount() != newSize) {
                    mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(newSize));
                }
            }
        });
    }

}
