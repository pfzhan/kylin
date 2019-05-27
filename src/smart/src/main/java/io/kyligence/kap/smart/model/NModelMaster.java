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

package io.kyligence.kap.smart.model;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.util.ConvertToComputedColumn;
import io.kyligence.kap.smart.NModelSelectProposer;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.NQueryRunnerFactory;
import io.kyligence.kap.smart.util.CubeUtils;

public class NModelMaster {

    private static final Logger LOGGER = LoggerFactory.getLogger(NModelMaster.class);

    private final NSmartContext.NModelContext modelContext;
    private final NProposerProvider proposerProvider;
    private final KylinConfig kylinConfig;
    private final String project;

    public NModelMaster(NSmartContext.NModelContext modelContext) {
        this.modelContext = modelContext;
        this.proposerProvider = NProposerProvider.create(modelContext);
        this.kylinConfig = modelContext.getSmartContext().getKylinConfig();
        this.project = modelContext.getSmartContext().getProject();
    }

    public NDataModel proposeInitialModel() {
        NDataModel dataModel = new NDataModel();
        dataModel.setRootFactTableName(modelContext.getModelTree().getRootFactTable().getIdentity());
        dataModel.setDescription(StringUtils.EMPTY);
        dataModel.setFilterCondition(StringUtils.EMPTY);
        dataModel.setPartitionDesc(new PartitionDesc());
        dataModel.setComputedColumnDescs(Lists.newArrayList());

        FunctionDesc countStar = CubeUtils.newCountStarFuncDesc(dataModel);
        NDataModel.Measure countStarMeasure = CubeUtils.newMeasure(countStar, "COUNT_ALL", NDataModel.MEASURE_ID_BASE);
        dataModel.setAllMeasures(Lists.newArrayList(countStarMeasure));
        return dataModel;
    }

    public NDataModel proposeJoins(NDataModel dataModel) {
        return proposerProvider.getJoinProposer().propose(dataModel);
    }

    public NDataModel proposeScope(NDataModel dataModel) {
        return proposerProvider.getScopeProposer().propose(dataModel);
    }

    public NDataModel proposePartition(NDataModel dataModel) {
        return proposerProvider.getPartitionProposer().propose(dataModel);
    }

    public NDataModel proposeComputedColumn(NDataModel dataModel) {
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        Set<String> transformers = Sets.newHashSet(kylinConfig.getQueryTransformers());
        boolean isComputedColumnEnabled = transformers.contains(ConvertToComputedColumn.class.getCanonicalName())
                && kapConfig.isImplicitComputedColumnConvertEnabled();
        if (!isComputedColumnEnabled) {
            return dataModel;
        }

        List<ComputedColumnDesc> originalCCs = Lists.newArrayList(dataModel.getComputedColumnDescs());
        try {
            dataModel = proposerProvider.getComputedColumnProposer().propose(dataModel);
            if (dataModel.getComputedColumnDescs().size() != originalCCs.size()) {
                // New CC detected, need to rebuild ModelContext regarding new coming CC
                updateContextWithCC(dataModel);
            }
        } catch (Exception e) {
            LOGGER.error("Propose failed, will discard new computed columns.", e);
            dataModel.setComputedColumnDescs(originalCCs);
        }
        return dataModel;
    }

    private void updateContextWithCC(NDataModel dataModel) {
        Map<String, String> mapNewAndOldQueries = Maps.newHashMap();
        for (OLAPContext olapContext : modelContext.getModelTree().getOlapContexts()) {
            String oldQuery = olapContext.sql;
            if (StringUtils.isEmpty(oldQuery)) {
                continue;
            }
            String newQuery = oldQuery;
            try {
                newQuery = new ConvertToComputedColumn().transformImpl(oldQuery, project, dataModel,
                        dataModel.getRootFactTable().getTableDesc().getDatabase());
            } catch (Exception e) {
                LOGGER.warn("NModelMaster.updateContextWithCC failed to transform query: {}", oldQuery, e);
            }
            mapNewAndOldQueries.put(newQuery, oldQuery);
        }

        if (mapNewAndOldQueries.isEmpty()) {
            return;
        }

        List<String> newQueries = Lists.newArrayList(mapNewAndOldQueries.keySet());
        List<String> oldQueries = newQueries.stream().map(mapNewAndOldQueries::get).collect(Collectors.toList());

        // Rebuild modelTrees and find match one to replace original
        try (AbstractQueryRunner extractor = NQueryRunnerFactory.createForModelSuggestion(kylinConfig,
                project, newQueries.toArray(new String[0]), Lists.newArrayList(dataModel), 1)) {
            extractor.execute();
            List<ModelTree> modelTrees = new GreedyModelTreesBuilder(kylinConfig, project,
                    modelContext.getSmartContext()) //
                    .build(oldQueries, extractor.getAllOLAPContexts(), null);
            ModelTree updatedModelTree = null;
            for (ModelTree modelTree : modelTrees) {
                if (NModelSelectProposer.matchModelTree(dataModel, modelTree)) {
                    updatedModelTree = modelTree;
                    break;
                }
            }
            if (updatedModelTree == null) {
                return;
            }

            // Update context info
            this.modelContext.setModelTree(updatedModelTree);
        } catch (Exception e) {
            LOGGER.warn("NModelMaster.updateContextWithCC failed to update model tree", e);
        }
    }
}
