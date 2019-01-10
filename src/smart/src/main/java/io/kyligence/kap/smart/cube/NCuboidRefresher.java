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

package io.kyligence.kap.smart.cube;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartContext.NModelContext;

class NCuboidRefresher extends NAbstractCubeProposer {

    private static final Logger logger = LoggerFactory.getLogger(NCuboidRefresher.class);

    private String draftVersion;
    private final NSmartContext smartContext;
    private FavoriteQueryManager favoriteQueryManager;

    NCuboidRefresher(NModelContext context) {
        super(context);
        smartContext = context.getSmartContext();
        draftVersion = smartContext.getDraftVersion();
        favoriteQueryManager = FavoriteQueryManager.getInstance(smartContext.getKylinConfig(), smartContext.getProject());
    }

    @Override
    public IndexPlan doPropose(IndexPlan indexPlan) {

        Preconditions.checkNotNull(draftVersion);
        final int semanticVersion = indexPlan.getModel().getSemanticVersion();

        // 1. get originalCuboidsMap
        Map<IndexEntity.IndexIdentifier, IndexEntity> originalCuboidsMap = indexPlan.getWhiteListIndexesMap();

        // 2. load favoriteQueryRealizations
        StringBuilder beforeRefreshLogBuilder = new StringBuilder();
        List<LayoutEntity> layouts = Lists.newArrayList();
        collectAllLayouts(originalCuboidsMap.values());
        layouts.forEach(layout -> beforeRefreshLogBuilder.append(layout.getId()).append(" ")); // debug log
        logger.debug("layouts before refresh: [{}]", beforeRefreshLogBuilder);
        Set<FavoriteQueryRealization> allFavoriteQueryRealizations = loadFavoriteQueryRealizations(layouts);

        // 3. validate semantic version
        if (!allFavoriteQueryRealizations.isEmpty()) {
            final FavoriteQueryRealization favoriteQueryRealization = allFavoriteQueryRealizations.iterator().next();
            final int semanticVersionOfFavoirteQuery = favoriteQueryRealization.getSemanticVersion();
            Preconditions.checkState(semanticVersion == semanticVersionOfFavoirteQuery,
                    "model semantic version has changed, no need to continue. "
                            + "current semantic version: {}, expected semantic version: {}",
                    semanticVersion, semanticVersionOfFavoirteQuery);
        }

        // 4. rebuild accelerationInfoMap
        smartContext.reBuildAccelerationInfoMap(allFavoriteQueryRealizations);

        // 5. delete unmodified cuboids in originalCuboidMap, collect favoriteQueryRealizations and delete them
        originalCuboidsMap.forEach((cuboidIdentifier, indexEntity) -> {
            final Iterator<LayoutEntity> iterator = indexEntity.getLayouts().iterator();
            while (iterator.hasNext()) {
                final LayoutEntity layout = iterator.next();
                if (layout.matchDraftVersion(draftVersion)) {
                    final Set<String> sqlPattern = smartContext.eraseLayoutInAccelerateInfo(layout);
                    sqlPattern.forEach(sql -> favoriteQueryManager.removeRealizations(sql));
                    iterator.remove();
                }
            }
        });

        // 6. propose cuboid again
        final CuboidSuggester cuboidSuggester = new CuboidSuggester(context, indexPlan, originalCuboidsMap);
        cuboidSuggester.suggestCuboids(context.getModelTree());

        // 7. publish all layouts
        StringBuilder afterRefreshLogBuilder = new StringBuilder();
        final Collection<IndexEntity> cuboids = originalCuboidsMap.values();
        cuboids.forEach(cuboid -> cuboid.getLayouts().forEach(layout -> {
            if (layout.matchDraftVersion(draftVersion)) {
                layout.publish();
            }
        }));
        indexPlan.setIndexes(Lists.newArrayList(cuboids));
        collectAllLayouts(cuboids).forEach(layout -> afterRefreshLogBuilder.append(layout.getId()).append(" ")); // debug log
        logger.debug("layouts after refresh: [{}]", afterRefreshLogBuilder);

        return indexPlan;
    }

    /**
     * load relations between favorite query and layout by layout
     */
    private Set<FavoriteQueryRealization> loadFavoriteQueryRealizations(List<LayoutEntity> layouts) {

        // TODO load favorite query realizations by batch of layouts
        Set<FavoriteQueryRealization> favoriteQueryRealizations = Sets.newHashSet();
        Preconditions.checkState(CollectionUtils.isEmpty(layouts));
        layouts.forEach(layout -> {
            final long layoutId = layout.getId();
            final String modelId = layout.getModel().getId();

            final List<FavoriteQueryRealization> byConditions = favoriteQueryManager.getRealizationsByConditions(modelId, layoutId);
            favoriteQueryRealizations.addAll(byConditions);
        });

        return favoriteQueryRealizations;
    }
}
