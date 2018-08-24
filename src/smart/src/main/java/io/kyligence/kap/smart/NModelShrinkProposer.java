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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NModelShrinkProposer extends NAbstractProposer {

    public NModelShrinkProposer(NSmartContext modelCtx) {
        super(modelCtx);
    }

    @Override
    void propose() {
        if (context.getModelContexts() == null)
            return;

        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.getOrigModel() == null) {
                continue;
            }
            if (modelCtx.getOrigCubePlan() == null) {
                continue;
            }
            if (modelCtx.getTargetCubePlan() == null) {
                continue;
            }

            NDataModel model = modelCtx.getTargetModel();
            Map<String, NCubePlan> modelCubePlans = Maps.newHashMap();

            List<NCubePlan> allCubePlans = NCubePlanManager.getInstance(context.getKylinConfig(), context.getProject()).listAllCubePlans();
            for (NCubePlan cubePlan : allCubePlans) {
                if (model.getName().equals(cubePlan.getModelName())) {
                    modelCubePlans.put(cubePlan.getName(), cubePlan);
                }
            }

            Map<Integer, NamedColumn> namedColumns = new HashMap<>();
            for (NamedColumn namedColumn : model.getAllNamedColumns()) {
                namedColumn.tomb = true;
                namedColumns.put(namedColumn.id, namedColumn);
            }
            Map<Integer, Measure> measures = new HashMap<>();
            for (Measure measure : model.getAllMeasures()) {
                if (measure.getFunction().isCount()) {
                    continue;
                }
                measure.tomb = true;
                measures.put(measure.id, measure);
            }

            NCubePlan targetCubePlan = modelCtx.getTargetCubePlan();
            modelCubePlans.put(targetCubePlan.getName(), targetCubePlan);

            for (NCubePlan cubePlan : modelCubePlans.values()) {
                for (NCuboidDesc cuboidDesc : cubePlan.getCuboids()) {
                    for (int id : cuboidDesc.getDimensions()) {
                        NamedColumn used = namedColumns.get(id);
                        if (used != null) {
                            used.tomb = false;
                        }
                    }
                    for (int id : cuboidDesc.getMeasures()) {
                        Measure used = measures.get(id);
                        if (used == null) {
                            continue;
                        }
                        used.tomb = false;
                        for (TblColRef param : used.getFunction().getParameter().getColRefs()) {
                            Integer paramId = model.getColId(param);
                            if (paramId == null) {
                                continue;
                            }
                            namedColumns.get(paramId).tomb = false;
                        }
                    }
                }
            }
            initModel(model);
        }

    }

    private void initModel(NDataModel modelDesc) {
        KylinConfig kylinConfig = context.getKylinConfig();
        String project = context.getProject();
        modelDesc.init(kylinConfig, NTableMetadataManager.getInstance(kylinConfig, project).getAllTablesMap(),
                Lists.<NDataModel>newArrayList(), false);
    }

}
