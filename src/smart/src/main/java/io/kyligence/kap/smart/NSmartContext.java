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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
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

    // only used in auto-modeling
    private String draftVersion;

    @Setter
    private List<NModelContext> modelContexts;
    @Setter
    private Map<String, AccelerateInfo> accelerateInfoMap;
    
    @Getter
    private Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();

    private final NTableMetadataManager tableMetadataManager;
    private final Map<String, TableExtDesc.ColumnStats> columnStatsCache = Maps.newConcurrentMap();

    /**
     * Erase the layout in accelerate info map
     * @param layout the layout need erase
     * @return corresponding sqlPatterns of this layout
     */
    public Set<String> eraseLayoutInAccelerateInfo(NCuboidLayout layout) {

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
     * generate a biMap of sql and it's hashcode
     */
    private BiMap<String, Integer> genSqlHashcodeMap() {
        BiMap<String, Integer> map = HashBiMap.create();
        for (String sql : this.sqls) {
            if (!map.containsKey(sql)) {
                map.put(sql, sql.hashCode());
            }
        }
        return map;
    }

    /**
     * Rebuild accelerationInfoMap by relations between favorite query and layout from database
     * @param favoriteQueryRealizations serialized relations between layout and favorite query
     */
    public void reBuildAccelerationInfoMap(Set<FavoriteQueryRealization> favoriteQueryRealizations) {
        final BiMap<String, Integer> sqlHashcodeMap = genSqlHashcodeMap();
        for (FavoriteQueryRealization fqRealization : favoriteQueryRealizations) {
            final String sql = sqlHashcodeMap.inverse().get(fqRealization.getSqlPatternHash());
            if (sql == null) {
                continue;
            }
            if (!accelerateInfoMap.containsKey(sql)) {
                accelerateInfoMap.put(sql, new AccelerateInfo());
            }

            if (accelerateInfoMap.containsKey(sql)) {
                val queryRelatedLayouts = accelerateInfoMap.get(sql).getRelatedLayouts();
                String modelId = fqRealization.getModelId();
                String cubePlanId = fqRealization.getCubePlanId();
                long layoutId = fqRealization.getCuboidLayoutId();
                int semanticVersion = fqRealization.getSemanticVersion();
                val queryLayoutRelation = new AccelerateInfo.QueryLayoutRelation(sql, modelId, cubePlanId, layoutId,
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
        private NDataModel origModel; // used when update existing models

        @Setter(AccessLevel.PACKAGE)
        private NCubePlan targetCubePlan;
        @Setter(AccessLevel.PACKAGE)
        private NCubePlan origCubePlan;

        private NSmartContext smartContext;

        private NModelContext(NSmartContext smartContext, ModelTree modelTree) {
            this.smartContext = smartContext;
            this.modelTree = modelTree;
        }
    }

    public NModelContext createModelContext(ModelTree modelTree) {
        return new NModelContext(this, modelTree);
    }

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls, String draftVersion) {
        this(kylinConfig, project, sqls);
        this.draftVersion = draftVersion;
    }

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        this.sqls = sqls;
        this.smartConfig = SmartConfig.wrap(this.kylinConfig);
        this.accelerateInfoMap = Maps.newHashMap();

        tableMetadataManager = NTableMetadataManager.getInstance(this.kylinConfig, project);
    }

    // =======================

    public TableExtDesc.ColumnStats getColumnStats(TblColRef colRef) {
        TableExtDesc.ColumnStats ret = columnStatsCache.get(colRef.getIdentity());
        if (ret != null)
            return ret;

        TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(colRef.getTableRef().getTableDesc());
        if (tableExtDesc != null && !tableExtDesc.getColumnStats().isEmpty()) {
            ret = tableExtDesc.getColumnStats().get(colRef.getColumnDesc().getZeroBasedIndex());
            columnStatsCache.put(colRef.getIdentity(), ret);
        } else {
            ret = null;
        }
        return ret;
    }
}
