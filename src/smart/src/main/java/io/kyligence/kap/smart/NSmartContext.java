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

package io.kyligence.kap.smart;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
public class NSmartContext {

    private final KylinConfig kylinConfig;
    private final SmartConfig smartConfig;
    private final String project;
    private final String[] sqls;
    // if false, it will create totally new model and won't reuse CC.
    private final boolean reuseExistedModel;

    private final boolean couldCreateNewModel;

    private boolean canModifyOriginModel = false;

    @Setter
    private boolean skipEvaluateCC;

    // only used in auto-modeling
    private String draftVersion;

    @Setter
    private List<NModelContext> modelContexts;
    @Setter
    private Map<String, AccelerateInfo> accelerateInfoMap;

    private final NTableMetadataManager tableMetadataManager;

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls, String draftVersion) {
        this(kylinConfig, project, sqls);
        this.draftVersion = draftVersion;
    }

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls, boolean reuseExistedModel,
            boolean couldCreateNewModel) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        this.sqls = sqls;
        this.smartConfig = SmartConfig.wrap(this.kylinConfig);
        this.accelerateInfoMap = Maps.newHashMap();
        this.reuseExistedModel = reuseExistedModel;

        tableMetadataManager = NTableMetadataManager.getInstance(this.kylinConfig, project);
        val prjInstance = NProjectManager.getInstance(this.kylinConfig).getProject(project);
        Preconditions.checkArgument(!prjInstance.isExpertMode() || couldCreateNewModel,
                "In Expert Mode, it CANNOT create new model !!!");
        this.couldCreateNewModel = couldCreateNewModel;
        this.canModifyOriginModel = prjInstance.isSmartMode();
    }

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        this.sqls = sqls;
        this.smartConfig = SmartConfig.wrap(this.kylinConfig);
        this.accelerateInfoMap = Maps.newHashMap();
        this.reuseExistedModel = true;
        val prjInstance = NProjectManager.getInstance(this.kylinConfig).getProject(project);
        this.couldCreateNewModel = prjInstance.isSmartMode();
        this.canModifyOriginModel = prjInstance.isSmartMode();

        tableMetadataManager = NTableMetadataManager.getInstance(this.kylinConfig, project);
    }

    /**
     * Erase the layout in accelerate info map
     * @param layout the layout need erase
     * @return corresponding sqlPatterns of this layout
     */
    public Set<String> eraseLayoutInAccelerateInfo(LayoutEntity layout) {

        Preconditions.checkNotNull(layout);
        Set<String> sqlPatterns = Sets.newHashSet();
        for (val entry : accelerateInfoMap.entrySet()) {

            val relatedLayouts = entry.getValue().getRelatedLayouts();
            val iterator = relatedLayouts.iterator();
            while (iterator.hasNext()) {

                // only when layoutId, cubePlanId, modelId and semanticVersion consist with queryLayoutRelation would do erase
                final AccelerateInfo.QueryLayoutRelation next = iterator.next();
                if (next.consistent(layout)) {
                    Preconditions.checkState(entry.getKey().equalsIgnoreCase(next.getSql())); // must equal, otherwise error
                    iterator.remove();
                    sqlPatterns.add(entry.getKey());
                }
            }
        }
        return sqlPatterns;
    }

    /**
     * Rebuild accelerationInfoMap by relations between favorite query and layout from database
     * @param favoriteQueryRealizations serialized relations between layout and favorite query
     */
    public void reBuildAccelerationInfoMap(Set<FavoriteQueryRealization> favoriteQueryRealizations) {
        final Set<String> sqlPatternsSet = new HashSet<>(Lists.newArrayList(this.sqls));
        for (FavoriteQueryRealization fqRealization : favoriteQueryRealizations) {
            final String sqlPattern = fqRealization.getSqlPattern();
            if (!sqlPatternsSet.contains(sqlPattern))
                continue;
            if (!accelerateInfoMap.containsKey(sqlPattern)) {
                accelerateInfoMap.put(sqlPattern, new AccelerateInfo());
            }

            if (accelerateInfoMap.containsKey(sqlPattern)) {
                val queryRelatedLayouts = accelerateInfoMap.get(sqlPattern).getRelatedLayouts();
                String modelId = fqRealization.getModelId();
                long layoutId = fqRealization.getLayoutId();
                int semanticVersion = fqRealization.getSemanticVersion();
                val queryLayoutRelation = new AccelerateInfo.QueryLayoutRelation(sqlPattern, modelId, layoutId,
                        semanticVersion);
                queryRelatedLayouts.add(queryLayoutRelation);
            }
        }
    }

    @Getter
    public static class NModelContext {
        @Setter
        private ModelTree modelTree; // query

        @Setter(AccessLevel.PACKAGE)
        private NDataModel targetModel; // output model
        @Setter(AccessLevel.PACKAGE)
        private NDataModel originModel; // used when update existing models

        @Setter(AccessLevel.PACKAGE)
        private IndexPlan targetIndexPlan;
        @Setter(AccessLevel.PACKAGE)
        private IndexPlan originIndexPlan;

        @Setter(AccessLevel.PACKAGE)
        private boolean snapshotSelected;

        private NSmartContext smartContext;
        private Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
        @Getter
        @Setter
        private boolean needUpdateCC = false;

        private NModelContext(NSmartContext smartContext, ModelTree modelTree) {
            this.smartContext = smartContext;
            this.modelTree = modelTree;
        }

        public boolean withoutTargetModel() {
            return this.targetModel == null;
        }

        public boolean withoutAnyIndexes() {
            // we can not modify rule_based_indexes
            return this.targetIndexPlan == null || CollectionUtils.isEmpty(this.targetIndexPlan.getIndexes());
        }
    }

    public NModelContext createModelContext(ModelTree modelTree) {
        return new NModelContext(this, modelTree);
    }

}
