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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import lombok.val;

public class IndexCleaner implements MetadataCleaner {

    public void cleanup(String project) {
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val favoriteQueryManager = FavoriteQueryManager.getInstance(config, project);

        for (val model : dataflowManager.listUnderliningDataModels()) {
            val dataflow = dataflowManager.getDataflow(model.getId());
            val autoLayouts = getAutoLayouts(dataflow);
            
            // notice here we do not check whether model's semantic version matches fq's semantic version
            // because model's semantic version could change either because 1: join changed or 2: partition column changed
            // for case 1, the layout actually loses reference but for case 2, the layout is still being referenced.
            // Since we make sure whether it's case 1 or case 2, we prefer to treat it as non garbage.

            // For case 1, When the next time FavoriteQueryAdjustWorker starts, the FQ will adjust to a new model.
            // before it happens, the FQ is actually not accelerated
            val referencedLayouts = favoriteQueryManager.getFQRByConditions(model.getId(), null).stream()
                    .map(FavoriteQueryRealization::getLayoutId).collect(Collectors.toSet());
            autoLayouts.removeAll(referencedLayouts);
            if (CollectionUtils.isNotEmpty(autoLayouts)) {
                cleanupIsolatedIndex(project, model.getId(), autoLayouts);
            }
        }
    }

    private Set<Long> getAutoLayouts(NDataflow dataflow) {
        val cube = dataflow.getIndexPlan();
        return cube.getWhitelistLayouts().stream().filter(layoutEntity -> !layoutEntity.isManual())
                .map(LayoutEntity::getId).collect(Collectors.toSet());
    }

    private void cleanupIsolatedIndex(String project, String modelId, Set<Long> garbageLayouts) {
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(),
                copyForWrite -> copyForWrite.removeLayouts(garbageLayouts, LayoutEntity::equals, true, false));
    }
}
