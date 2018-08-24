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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NSmartMaster {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSmartMaster.class);

    private NSmartContext context;
    private NProposerProvider proposerProvider;

    public NSmartMaster(KylinConfig kylinConfig, String project, String[] sqls) {
        this.context = new NSmartContext(kylinConfig, project, sqls);
        this.proposerProvider = NProposerProvider.create(context);
    }

    public NSmartContext getContext() {
        return context;
    }

    public void analyzeSQLs() {
        proposerProvider.getSQLAnalysisProposer().propose();
    }

    public void selectModel() {
        proposerProvider.getModelSelectProposer().propose();
    }

    public void optimizeModel() {
        proposerProvider.getModelOptProposer().propose();
    }

    public void selectCubePlan() {
        proposerProvider.getCubePlanSelectProposer().propose();
    }

    public void optimizeCubePlan() {
        proposerProvider.getCubePlanOptProposer().propose();
    }
    
    public void shrinkCubePlan() {
        proposerProvider.getCubePlanShrinkProposer().propose();
    }
    
    public void shrinkModel() {
        proposerProvider.getModelShrinkProposer().propose();
    }

    public void runAll() throws IOException {
        analyzeSQLs();
        selectModel();
        optimizeModel();
        saveModel();

        selectCubePlan();
        optimizeCubePlan();
        saveCubePlan();
    }

    public void saveCubePlan() throws IOException {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(context.getKylinConfig(), context.getProject());
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(context.getKylinConfig(), context.getProject());
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            if (cubePlanManager.getCubePlan(cubePlan.getName()) != null) {
                cubePlan = cubePlanManager.updateCubePlan(cubePlan);

                NDataflow df = dataflowManager.getDataflow(cubePlan.getName());
                NDataflowUpdate update = new NDataflowUpdate(df.getName());
                List<NDataCuboid> toAddCuboids = Lists.newArrayList();

                for (NDataSegment seg : df.getSegments()) {
                    Map<Long, NDataCuboid> cuboidMap = seg.getCuboidsMap();
                    for (NCuboidDesc desc : cubePlan.getCuboids()) {
                        for (NCuboidLayout layout : desc.getLayouts()) {
                            if (!cuboidMap.containsKey(layout.getId())) {
                                toAddCuboids.add(NDataCuboid.newDataCuboid(df, seg.getId(), layout.getId()));
                            }
                        }
                    }
                }

                update.setToAddOrUpdateCuboids(toAddCuboids.toArray(new NDataCuboid[0]));
                dataflowManager.updateDataflow(update);
            } else {
                cubePlanManager.createCubePlan(cubePlan);
                dataflowManager.createDataflow(cubePlan.getName(), context.getProject(), cubePlan, null);
            }
        }
    }

    public void saveModel() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(context.getKylinConfig(), context.getProject());
        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            NDataModel model = modelCtx.getTargetModel();
            if (modelManager.getDataModelDesc(model.getName()) != null) {
                modelManager.updateDataModelDesc(model);
            } else {
                modelManager.createDataModelDesc(model, null);
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
