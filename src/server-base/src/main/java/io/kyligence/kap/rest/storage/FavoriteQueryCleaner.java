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
package io.kyligence.kap.rest.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FavoriteQueryCleaner implements GarbageCleaner {

    private final ProjectInstance project;

    private Set<String> inactiveFavoriteSqlPatterns = new HashSet<>();

    private List<CubeCleanInfo> usedLayouts = Lists.newArrayList();

    public FavoriteQueryCleaner(ProjectInstance project) {
        this.project = project;
    }

    @Override
    public void collect(NDataModel model) {
        val favoriteQueryManager = FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());
        val cube = getCube(model);
        val layoutIds = getLayouts(cube);

        val allRealizations = favoriteQueryManager.getRealizationsByConditions(model.getId(), null);

        val outdatedRealizations = allRealizations.stream().filter(
                r -> r.getSemanticVersion() != model.getSemanticVersion() || !layoutIds.contains(r.getLayoutId()))
                .collect(Collectors.toList());

        val outdatedFqsInModel = new HashSet<String>();
        // all realizations in fq is useless, even though it's layoutid is exist;
        for (FavoriteQueryRealization unused : outdatedRealizations) {
            outdatedFqsInModel.add(unused.getSqlPattern());
        }

        val usedIds = allRealizations.stream().filter(r -> !outdatedRealizations.contains(r))
                .map(FavoriteQueryRealization::getLayoutId).collect(Collectors.toSet());
        usedLayouts.add(new CubeCleanInfo(cube.getUuid(), usedIds));

        inactiveFavoriteSqlPatterns.addAll(outdatedFqsInModel);
    }

    @Override
    public void cleanup() throws Exception {
        val fqManager = FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());

        for (String sqlPattern : inactiveFavoriteSqlPatterns) {
            fqManager.updateStatus(sqlPattern, FavoriteQueryStatusEnum.WAITING, null);
            fqManager.removeRealizations(sqlPattern);
        }

        val cubeManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());
        for (CubeCleanInfo entry : usedLayouts) {
            cubeManager.updateIndexPlan(entry.getCubeName(), copyForWrite -> {
                val autoLayoutIds = copyForWrite.getWhitelistLayouts().stream().filter(LayoutEntity::isAuto)
                        .map(LayoutEntity::getId).collect(Collectors.toSet());
                autoLayoutIds.removeAll(entry.getUsedIds());
                log.debug("these layouts are useless {}", autoLayoutIds);
                copyForWrite.removeLayouts(autoLayoutIds, LayoutEntity::equals, true, false);
            });
        }
    }

    @Data
    @AllArgsConstructor
    static class CubeCleanInfo {

        private String cubeName;

        private Set<Long> usedIds;
    }
}
