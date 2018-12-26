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

package io.kyligence.kap.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AccelerateInfo.QueryLayoutRelation;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

public class RecAndQueryCompareUtil {

    private RecAndQueryCompareUtil() {
    }

    public static String writeQueryLayoutRelationAsString(KylinConfig kylinConfig, String project,
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts) {
        if (CollectionUtils.isEmpty(relatedLayouts)) {
            return "[ ]";
        }

        List<String> list = Lists.newArrayList();
        relatedLayouts.forEach(queryLayoutRelation -> {
            List<String> colOrderNames = Lists.newArrayList();

            final NCubePlan cubePlan = NCubePlanManager.getInstance(kylinConfig, project)
                    .getCubePlan(queryLayoutRelation.getCubePlanId());
            Preconditions.checkNotNull(cubePlan);
            final NCuboidLayout cuboidLayout = cubePlan.getCuboidLayout(queryLayoutRelation.getLayoutId());
            final ImmutableList<Integer> colOrder = cuboidLayout.getColOrder();
            final BiMap<Integer, TblColRef> effectiveDimCols = cuboidLayout.getCuboidDesc().getEffectiveDimCols();
            final ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures = cuboidLayout.getCuboidDesc()
                    .getEffectiveMeasures();
            colOrder.forEach(column -> {
                if (column < NDataModel.MEASURE_ID_BASE) {
                    colOrderNames.add(effectiveDimCols.get(column).getName());
                } else {
                    colOrderNames.add(effectiveMeasures.get(column).getName());
                }
            });
            String tmp = String.format("{model=%s,cubePlan=%s,layout=%s,colOrderName=[%s]}",
                    queryLayoutRelation.getModelId(), queryLayoutRelation.getCubePlanId(),
                    queryLayoutRelation.getLayoutId(), String.join(",", colOrderNames));
            list.add(tmp);
        });

        return "[" + String.join(",", list) + "]";
    }

    /**
     * compute the compare level of propose result and query result
     */
    public static void computeCompareRank(KylinConfig kylinConfig, String project,
            Map<String, CompareEntity> compareEntityMap) {

        compareEntityMap.forEach((sql, entity) -> {
            if (entity.getLevel() == AccelerationMatchedLevel.FAILED_QUERY) {
                return;
            }

            final Collection<OLAPContext> olapContexts = entity.getOlapContexts();
            Set<AccelerateInfo.QueryLayoutRelation> layouts = Sets.newHashSet();
            Set<Long> cuboidIds = Sets.newHashSet();
            Set<String> modelIds = Sets.newHashSet();

            try {
                olapContexts.forEach(olapContext -> {
                    if (olapContext.storageContext.isUseSnapshot()) {
                        entity.setLevel(AccelerationMatchedLevel.SNAPSHOT_QUERY);
                        return;
                    }

                    final NCuboidLayout cuboidLayout = olapContext.storageContext.getCandidate().getCuboidLayout();
                    final String modelId = cuboidLayout.getModel().getName();
                    final String cubePlanId = cuboidLayout.getCuboidDesc().getCubePlan().getName();
                    final long layoutId = cuboidLayout.getId();
                    final int semanticVersion = cuboidLayout.getModel().getSemanticVersion();

                    QueryLayoutRelation relation = new QueryLayoutRelation(sql, modelId, cubePlanId, layoutId,
                            semanticVersion);
                    layouts.add(relation);
                    cuboidIds.add(cuboidLayout.getCuboidDesc().getId());
                    modelIds.add(modelId);
                });
                entity.setQueryUsedLayouts(writeQueryLayoutRelationAsString(kylinConfig, project, layouts));
            } catch (Exception e) {
                entity.setLevel(AccelerationMatchedLevel.SIMPLE_QUERY);
                return;
            }

            if (entity.getLevel() == AccelerationMatchedLevel.SNAPSHOT_QUERY) {
                return;
            } else if (Objects.equals(entity.getAccelerateInfo().getRelatedLayouts(), layouts)) {
                entity.setLevel(AccelerationMatchedLevel.ALL_MATCH);
                return;
            }

            final Set<QueryLayoutRelation> relatedLayouts = entity.getAccelerateInfo().getRelatedLayouts();
            Set<String> proposedModelIds = Sets.newHashSet();
            Set<Long> proposedCuboidIds = Sets.newHashSet();
            relatedLayouts.forEach(layout -> {
                proposedModelIds.add(layout.getModelId());
                proposedCuboidIds.add(layout.getLayoutId() - layout.getLayoutId() % NCuboidDesc.CUBOID_DESC_ID_STEP);
            });

            if (Objects.equals(cuboidIds, proposedCuboidIds)) {
                entity.setLevel(AccelerationMatchedLevel.LAYOUT_NOT_MATCH);
            } else if (Objects.equals(modelIds, proposedModelIds)) {
                entity.setLevel(AccelerationMatchedLevel.CUBOID_NOT_MATCH);
            } else if (entity.getAccelerateInfo().isBlocked()) {
                entity.setLevel(AccelerationMatchedLevel.BLOCKED_QUERY);
            } else {
                entity.setLevel(AccelerationMatchedLevel.MODEL_NOT_MATCH);
            }

        });
    }

    /**
     * summarize rank info
     */
    public static Map<AccelerationMatchedLevel, AtomicInteger> summarizeRankInfo(Map<String, CompareEntity> map) {
        Map<AccelerationMatchedLevel, AtomicInteger> compareResult = Maps.newLinkedHashMap();
        Arrays.stream(AccelerationMatchedLevel.values())
                .forEach(level -> compareResult.putIfAbsent(level, new AtomicInteger()));
        map.values().stream().map(CompareEntity::getLevel).map(compareResult::get)
                .forEach(AtomicInteger::incrementAndGet);
        return compareResult;
    }

    @Getter
    @Setter
    public static class CompareEntity {

        private String sql;
        @ToString.Exclude
        private Collection<OLAPContext> olapContexts;
        @ToString.Exclude
        private AccelerateInfo accelerateInfo;
        private String accelerateLayouts;
        private String queryUsedLayouts;
        private AccelerationMatchedLevel level;

        @Override
        public String toString() {
            return "CompareEntity{\n\tsql=[" + QueryUtil.removeCommentInSql(sql) + "],\n\taccelerateLayouts="
                    + accelerateLayouts + ",\n\tqueryUsedLayouts=" + queryUsedLayouts + ",\n\tlevel=" + level + "\n}";
        }
    }

    /**
     * Acceleration matched level
     */
    public enum AccelerationMatchedLevel {

        /**
         * simple query does not need realization
         */
        SIMPLE_QUERY,

        /**
         * query blocked in stage of propose cuboids and layouts
         */
        BLOCKED_QUERY,

        /**
         * failed in matching realizations
         */
        FAILED_QUERY,

        /**
         * query used snapshot or partly used snapshot
         */
        SNAPSHOT_QUERY,

        /**
         * all matched
         */
        ALL_MATCH,

        /**
         * cuboid matched, but layout not matched
         */
        LAYOUT_NOT_MATCH,

        /**
         * model matched, but cuboid not matched
         */
        CUBOID_NOT_MATCH,

        /**
         * model not matched
         */
        MODEL_NOT_MATCH

    }

}
