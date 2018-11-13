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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealizationJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FavoriteQueryCleaner implements GarbageCleaner {

    private final ProjectInstance project;

    private List<FavoriteQuery> inactiveFavoriteQueries = Lists.newArrayList();

    private List<FavoriteQueryRealization> inactiveRealizations = Lists.newArrayList();

    private List<CubeCleanInfo> usedLayouts = Lists.newArrayList();

    public FavoriteQueryCleaner(ProjectInstance project) {
        this.project = project;
    }

    @Override
    public void collect(NDataModel model) {
        val favoriteQueryRealizationDao = FavoriteQueryRealizationJDBCDao.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        val favoriteQueryDao = FavoriteQueryJDBCDao.getInstance(KylinConfig.getInstanceFromEnv());
        val cube = getCube(model);
        val layoutIds = getLayouts(cube);

        val allRealizations = favoriteQueryRealizationDao.getByConditions(model.getId(), cube.getId(), null);

        val outdatedRealizations = allRealizations.stream().filter(
                r -> r.getSemanticVersion() != model.getSemanticVersion() || !layoutIds.contains(r.getCuboidLayoutId()))
                .collect(Collectors.toList());

        val outdatedFqsInModel = Lists.<FavoriteQuery> newArrayList();
        for (FavoriteQueryRealization unused : outdatedRealizations) {
            val fq = favoriteQueryDao.getFavoriteQuery(unused.getSqlPatternHash(), model.getProject());
            if (fq == null) {
                continue;
            }
            fq.setStatus(FavoriteQueryStatusEnum.WAITING);
            outdatedFqsInModel.add(fq);
        }

        // all realizations in fq is useless, even though it's layoutid is exist;
        val outdatedRealizationsInModel = Lists.<FavoriteQueryRealization> newArrayList();
        for (FavoriteQuery fq : outdatedFqsInModel) {
            val realizations = favoriteQueryRealizationDao.getBySqlPatternHash(fq.getSqlPatternHash());
            outdatedRealizationsInModel.addAll(realizations);
        }

        val usedIds = allRealizations.stream().filter(r -> !outdatedRealizationsInModel.contains(r))
                .map(FavoriteQueryRealization::getCuboidLayoutId).collect(Collectors.toSet());
        usedLayouts.add(new CubeCleanInfo(cube.getName(), usedIds));

        inactiveRealizations.addAll(outdatedRealizationsInModel);
        inactiveFavoriteQueries.addAll(outdatedFqsInModel);
    }

    @Override
    public void cleanup() throws Exception {
        val fqDao = FavoriteQueryJDBCDao.getInstance(KylinConfig.getInstanceFromEnv());
        fqDao.batchUpdateStatus(inactiveFavoriteQueries);

        val fqrDao = FavoriteQueryRealizationJDBCDao.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());
        fqrDao.batchDelete(inactiveRealizations);

        val cubeManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName());
        for (CubeCleanInfo entry : usedLayouts) {
            cubeManager.updateCubePlan(entry.getCubeName(), copyForWrite -> {
                val autoLayoutIds = copyForWrite.getWhitelistCuboidLayouts().stream().filter(NCuboidLayout::isAuto)
                        .map(NCuboidLayout::getId).collect(Collectors.toSet());
                autoLayoutIds.removeAll(entry.getUsedIds());
                log.debug("these layouts are useless {}", autoLayoutIds);
                copyForWrite.removeLayouts(autoLayoutIds, NCuboidLayout::equals, true, false);
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
