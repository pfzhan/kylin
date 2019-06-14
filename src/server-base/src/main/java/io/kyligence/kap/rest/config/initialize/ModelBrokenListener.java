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

package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class ModelBrokenListener {

    @Subscribe
    public void onModelBrolken(NDataModel.ModelBrokenEvent event) {
        val origin = event.getModel();
        val project = origin.getProject();
        if (origin.isModelBroken()) {
            return;
        }

        UnitOfWork.doInTransactionWithRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);
            val originModel = modelManager.getDataModelDesc(origin.getId());
            if (originModel == null || !originModel.isBroken()) {
                return null;
            }
            val model = getBrokenModel(project, originModel.getResourcePath());
            if (model.isModelBroken()) {
                return null;
            }
            val dataflowManager = NDataflowManager.getInstance(config, project);
            val indexPlanManager = NIndexPlanManager.getInstance(config, project);
            val projectManager = NProjectManager.getInstance(config);

            if (projectManager.getProject(project).getMaintainModelType() == MaintainModelType.AUTO_MAINTAIN
                    && config.getSmartModeBrokenModelDeleteEnabled()) {
                dataflowManager.dropDataflow(model.getId());
                indexPlanManager.dropIndexPlan(model.getId());
                modelManager.dropModel(model);
                return null;
            }
            val dataflow = dataflowManager.getDataflow(origin.getId());
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
            dfUpdate.setStatus(RealizationStatusEnum.BROKEN);
            dataflowManager.updateDataflow(dfUpdate);

            model.setModelBroken(true);
            modelManager.updateDataBrokenModelDesc(model);
            return null;
        }, project);
    }

    @Subscribe
    public void onModelRepair(NDataModel.ModelRepairEvent event) {
        val origin = event.getModel();
        val project = origin.getProject();
        if (!origin.isModelBroken()) {
            return;
        }

        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc(origin.getId());
            if (!model.isModelBroken()) {
                return null;
            }
            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val dataflow = dataflowManager.getDataflow(origin.getId());
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(dfUpdate);
            if (model.getManagementType() == ManagementType.MODEL_BASED && model.getPartitionDesc() == null) {
                dataflowManager.fillDfManually(dataflow,
                        Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
            } else if (model.getManagementType() == ManagementType.TABLE_ORIENTED) {
                dataflowManager.fillDf(dataflow);
            }
            model.setModelBroken(false);
            modelManager.updateDataBrokenModelDesc(model);
            val eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
            addCuboidEvent.setModelId(model.getId());
            addCuboidEvent.setJobId(UUID.randomUUID().toString());
            addCuboidEvent.setOwner("ADMIN");
            eventManager.post(addCuboidEvent);

            PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
            postAddCuboidEvent.setModelId(model.getId());
            postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
            postAddCuboidEvent.setOwner("ADMIN");

            eventManager.post(postAddCuboidEvent);
            return null;
        }, project);
    }

    private NDataModel getBrokenModel(String project, String resourcePath) {
        try {
            val resource = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                    .getResource(resourcePath);
            val modelDesc = JsonUtil.readValue(resource.getByteSource().read(), NDataModel.class);
            modelDesc.setBroken(true);
            modelDesc.setProject(project);
            modelDesc.setMvcc(resource.getMvcc());
            return modelDesc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
