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
import java.util.UUID;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;
import io.kyligence.kap.query.util.SqlNodeExtractor;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public abstract class AbstractContext implements IKeep {

    private final SmartConfig smartConfig;
    private final String project;
    private final String[] sqlArray;
    private final ChainedProposer preProcessProposers;
    private final ChainedProposer processProposers;
    private final ExtraMetaInfo extraMeta = new ExtraMetaInfo();

    private final List<NDataModel> relatedModels = Lists.newArrayList();
    private final Set<String> relatedTables = Sets.newHashSet();

    @Setter
    private boolean isRestoredProposeContext;

    @Setter
    protected boolean canCreateNewModel;

    @Setter
    private List<ModelContext> modelContexts = Lists.newArrayList();
    @Setter
    private Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();
    @Getter(lazy = true)
    private final Map<String, RawRecItem> existingNonLayoutRecItemMap = Maps.newHashMap();

    @Setter
    private boolean skipEvaluateCC;
    protected boolean partialMatch;

    protected AbstractContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this.smartConfig = SmartConfig.wrap(kylinConfig);
        this.project = project;
        this.sqlArray = sqlArray;
        this.preProcessProposers = createPreProcessProposers();
        this.processProposers = createTransactionProposers();
        this.partialMatch = false;
        filterSqlRelatedModelsAndTables();
    }

    public ModelContext createModelContext(ModelTree modelTree) {
        return new ModelContext(this, modelTree);
    }

    public abstract IndexPlan getOriginIndexPlan(String modelId);

    public abstract List<NDataModel> getOriginModels();

    public abstract void changeModelMainType(NDataModel model);

    public abstract ChainedProposer createTransactionProposers();

    public abstract ChainedProposer createPreProcessProposers();

    public abstract void saveMetadata();

    public abstract String getIdentifier();

    private void filterSqlRelatedModelsAndTables() {
        Set<NDataModel> models = Sets.newHashSet();
        Set<String> tableIdentities = Sets.newHashSet();
        Map<String, Set<NDataModel>> tableToModelsMap = Maps.newHashMap();
        getAllModels().forEach(model -> {
            if (model.isBroken()) {
                return;
            }
            for (TableRef tableRef : model.getAllTables()) {
                tableToModelsMap.putIfAbsent(tableRef.getTableIdentity(), Sets.newHashSet());
                tableToModelsMap.get(tableRef.getTableIdentity()).add(model);
            }
        });

        Map<String, Set<String>> allTableMap = getProjectTableMap();
        if (!smartConfig.skipUselessMetadata() || isRestoredProposeContext) {
            tableToModelsMap.forEach((k, modelSet) -> getRelatedModels().addAll(modelSet));
            allTableMap.forEach((k, tableSet) -> getRelatedTables().addAll(tableSet));
            return;
        }

        // related tables from sql + related tables from baseModels
        Preconditions.checkNotNull(sqlArray);
        for (String sql : sqlArray) {
            Set<String> sqlRelatedTableIdentities = extractTables(sql, allTableMap);
            tableIdentities.addAll(sqlRelatedTableIdentities);
            sqlRelatedTableIdentities.forEach(tableIdentity -> {
                Set<NDataModel> relatedModels = tableToModelsMap.getOrDefault(tableIdentity, Sets.newHashSet());
                relatedModels.forEach(model -> {
                    Set<TableRef> allTables = model.getAllTables();
                    allTables.forEach(tableRef -> tableIdentities.add(tableRef.getTableIdentity()));
                });
                models.addAll(relatedModels);
            });
        }
        getRelatedModels().addAll(models);
        getRelatedTables().addAll(tableIdentities);
    }

    private Map<String, Set<String>> getProjectTableMap() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(smartConfig.getKylinConfig(), project);
        List<TableDesc> tableList = tableMgr.listAllTables();
        Map<String, Set<String>> tableNameMap = Maps.newHashMap();
        tableList.forEach(table -> {
            tableNameMap.putIfAbsent(table.getName(), Sets.newHashSet());
            tableNameMap.putIfAbsent(table.getIdentity(), Sets.newHashSet());
            tableNameMap.get(table.getName()).add(table.getIdentity());
            tableNameMap.get(table.getIdentity()).add(table.getIdentity());
        });
        return tableNameMap;
    }

    private Set<String> extractTables(String sql, Map<String, Set<String>> tableNameMap) {
        String normalizedSql = QueryUtil.normalizeForTableDetecting(project, sql);
        Set<String> allRelatedTables = Sets.newHashSet();
        try {
            List<SqlIdentifier> allSqlIdentifier = SqlNodeExtractor.getAllSqlIdentifier(normalizedSql);
            allSqlIdentifier.forEach(id -> {
                Set<String> orDefault = tableNameMap.getOrDefault(id.toString(), Sets.newHashSet());
                allRelatedTables.addAll(orDefault);
            });
        } catch (SqlParseException e) {
            log.info("extract error, sql is: {}", sql, e);
            AccelerateInfo accelerateInfo = new AccelerateInfo();
            accelerateInfo.setFailedCause(e);
            accelerateInfoMap.put(sql, accelerateInfo);
        }
        return allRelatedTables;
    }

    protected List<NDataModel> getAllModels() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).listAllModels();
    }

    public void recordException(ModelContext modelCtx, Exception e) {
        modelCtx.getModelTree().getOlapContexts().forEach(olapCtx -> {
            String sql = olapCtx.sql;
            final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            accelerateInfo.setFailedCause(e);
        });
    }

    public boolean needCollectRecommendations() {
        return this instanceof ModelReuseContextOfSemiV2;
    }

    public void handleExceptionAfterModelSelect() {
        // default do nothing 
    }

    public List<NDataModel> getProposedModels() {
        if (CollectionUtils.isEmpty(modelContexts)) {
            return Lists.newArrayList();
        }

        List<NDataModel> models = Lists.newArrayList();
        for (ModelContext modelContext : modelContexts) {
            NDataModel model = modelContext.getTargetModel();
            if (model == null)
                continue;

            models.add(modelContext.getTargetModel());
        }

        return models;
    }

    @Getter
    public static class ModelContext implements IKeep {
        @Setter
        private ModelTree modelTree; // query

        @Setter
        private NDataModel targetModel; // output model
        @Setter
        private NDataModel originModel; // used when update existing models

        @Setter
        private IndexPlan targetIndexPlan;
        @Setter
        private IndexPlan originIndexPlan;

        @Setter
        private Map<String, CCRecItemV2> ccRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, DimensionRecItemV2> dimensionRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, MeasureRecItemV2> measureRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, LayoutRecItemV2> indexRexItemMap = Maps.newHashMap();

        @Setter
        private boolean snapshotSelected;

        private final AbstractContext proposeContext;
        private final Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
        @Setter
        private boolean needUpdateCC = false;
        @Getter(lazy = true)
        private final Map<String, String> uniqueContentToFlag = loadUniqueContentToFlag();
        @Setter
        private ExcludedLookupChecker checker;

        private Map<String, String> loadUniqueContentToFlag() {
            Map<String, String> result = Maps.newHashMap();
            if (!(getProposeContext() instanceof AbstractSemiContextV2) || getTargetModel() == null) {
                return result;
            }

            String modelId = getTargetModel().getUuid();
            getProposeContext().getExistingNonLayoutRecItemMap().forEach((uniqueFlag, item) -> {
                if (item.getModelID().equalsIgnoreCase(modelId)) {
                    result.put(item.getRecEntity().getUniqueContent(), uniqueFlag);
                }
            });
            return result;
        }

        public ModelContext(AbstractContext proposeContext, ModelTree modelTree) {
            this.proposeContext = proposeContext;
            this.modelTree = modelTree;
        }

        public boolean isTargetModelMissing() {
            return targetModel == null;
        }

        public boolean isProposedIndexesEmpty() {
            // we can not modify rule_based_indexes
            return targetIndexPlan == null || CollectionUtils.isEmpty(targetIndexPlan.getIndexes());
        }

        public boolean skipSavingMetadata() {
            return isTargetModelMissing() || isProposedIndexesEmpty() || snapshotSelected;
        }

        /**
         * Only for Semi-Auto
         */
        public void gatherLayoutRecItem(LayoutEntity layout) {
            if (!getProposeContext().needCollectRecommendations()) {
                return;
            }
            LayoutRecItemV2 item = new LayoutRecItemV2();
            item.setLayout(layout);
            item.setCreateTime(System.currentTimeMillis());
            item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
            item.setUuid(UUID.randomUUID().toString());
            getIndexRexItemMap().put(layout.genUniqueContent(), item);
        }
    }

    @Getter
    @Setter
    public static class ExtraMetaInfo {

        private String modelOptRule;
        private Set<String> excludedTables = Sets.newHashSet();
        private Set<String> allModels = Sets.newHashSet();
        private Set<String> onlineModelIds = Sets.newHashSet();
    }
}
