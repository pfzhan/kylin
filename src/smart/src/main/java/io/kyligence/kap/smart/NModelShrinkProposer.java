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
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;

public class NModelShrinkProposer extends NAbstractProposer {

    public NModelShrinkProposer(NSmartContext modelCtx) {
        super(modelCtx);
    }

    @Override
    void propose() {
        if (context.getModelContexts() == null)
            return;

        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.getOrigModel() == null || modelCtx.getOrigCubePlan() == null
                    || modelCtx.getTargetCubePlan() == null) {
                continue;
            }

            NDataModel model = modelCtx.getTargetModel();
            Map<Integer, NamedColumn> namedColumnsById = Maps.newHashMap();
            Map<String, NamedColumn> namedColumnsByName = Maps.newHashMap();
            Map<Integer, Measure> measures = Maps.newHashMap();
            truncateModel(model, namedColumnsById, namedColumnsByName, measures);

            Map<String, NCubePlan> modelCubePlans = Maps.newHashMap();
            List<NCubePlan> allCubePlans = NCubePlanManager.getInstance(context.getKylinConfig(), context.getProject())
                    .listAllCubePlans();
            for (NCubePlan cubePlan : allCubePlans) {
                if (model.getName().equals(cubePlan.getModelName())) {
                    modelCubePlans.put(cubePlan.getName(), cubePlan);
                }
            }
            NCubePlan targetCubePlan = modelCtx.getTargetCubePlan();
            modelCubePlans.put(targetCubePlan.getName(), targetCubePlan);
            refillModel(modelCubePlans, namedColumnsById, namedColumnsByName, measures);

            initModel(model);
        }

    }

    private void truncateModel(NDataModel model, Map<Integer, NamedColumn> colsById,
            Map<String, NamedColumn> colsByName, Map<Integer, Measure> measures) {
        for (NamedColumn namedColumn : model.getAllNamedColumns()) {
            namedColumn.setStatus(NDataModel.ColumnStatus.TOMB);
            colsById.put(namedColumn.getId(), namedColumn);
            colsByName.put(namedColumn.getAliasDotColumn(), namedColumn);
        }
        for (Measure measure : model.getAllMeasures()) {
            if (measure.getFunction().isCount()) {
                continue;
            }
            measure.tomb = true;
            measures.put(measure.id, measure);
        }

        // Keep partition column in named columns
        PartitionDesc partitionDesc = model.getPartitionDesc();
        if (partitionDesc != null && partitionDesc.getPartitionDateColumn() != null) {
            String partitionColName = partitionDesc.getPartitionDateColumn();
            if (colsByName.containsKey(partitionColName)) {
                NamedColumn namedColumn = colsByName.get(partitionColName);
                namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
            }
        }
    }
    
    private void refillModel(Map<String, NCubePlan> modelCubePlans, Map<Integer, NamedColumn> colsById,
            Map<String, NamedColumn> colsByName, Map<Integer, Measure> measures) {
        Set<NamedColumn> usedCols = Sets.newHashSet();
        Set<Measure> usedMeasures = Sets.newHashSet();
        for (NCubePlan cubePlan : modelCubePlans.values()) {
            for (NCuboidDesc cuboidDesc : cubePlan.getAllCuboids()) {
                for (int id : cuboidDesc.getDimensions()) {
                    usedCols.add(colsById.get(id));
                }
                for (int id : cuboidDesc.getMeasures()) {
                    usedMeasures.add(measures.get(id));
                }
            }
        }
        
        usedMeasures.remove(null);
        for (Measure used : usedMeasures) {
            used.tomb = false;
            for (TblColRef param : used.getFunction().getParameter().getColRefs()) {
                usedCols.add(colsByName.get(param.getIdentity()));
            }
        }
        usedCols.remove(null);
        for (NamedColumn used : usedCols) {
            used.setStatus(NDataModel.ColumnStatus.DIMENSION);
        }
    }

    private void initModel(NDataModel modelDesc) {
        KylinConfig kylinConfig = context.getKylinConfig();
        String project = context.getProject();
        modelDesc.init(kylinConfig, NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap(),
                Lists.<NDataModel>newArrayList(), false);
    }

}
