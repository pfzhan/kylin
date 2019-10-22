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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.val;

public class ModelBrokenListener {

    private static final Logger logger = LoggerFactory.getLogger(ModelBrokenListener.class);

    private boolean needHandleModelBroken(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        if (model != null && model.isBroken() && !model.isHandledAfterBroken()) {
            return true;
        }

        return false;
    }

    @Subscribe
    public void onModelBroken(NDataModel.ModelBrokenEvent event) {
        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelBroken(project, modelId)) {
            return;
        }

        UnitOfWork.doInTransactionWithRetry(() -> {

            if (!needHandleModelBroken(project, modelId)) {
                return null;
            }

            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);

            val model = getBrokenModel(project, NDataModel.concatResourcePath(modelId, project));

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
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
            dfUpdate.setStatus(RealizationStatusEnum.BROKEN);
            dataflowManager.updateDataflow(dfUpdate);

            if (model.getBrokenReason() == NDataModel.BrokenReason.EVENT) {
                dataflowManager.updateDataflow(model.getId(), copyForWrite -> copyForWrite.setEventError(true));
            }
            model.setHandledAfterBroken(true);
            modelManager.updateDataBrokenModelDesc(model);
            EventDao eventDao = EventDao.getInstance(config, project);
            eventDao.getEventsByModel(modelId).stream().map(Event::getId).forEach(eventDao::deleteEvent);

            val recommendationManager = OptimizeRecommendationManager.getInstance(config, project);
            recommendationManager.cleanAll(model.getId());
            return null;
        }, project);
    }

    private boolean needHandleModelRepair(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        if (model != null && !model.isBroken() && model.isHandledAfterBroken()) {
            return true;
        }

        return false;
    }

    @Subscribe
    public void onModelRepair(NDataModel.ModelRepairEvent event) {

        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelRepair(project, modelId)) {
            return;
        }

        UnitOfWork.doInTransactionWithRetry(() -> {

            if (!needHandleModelRepair(project, modelId)) {
                return null;
            }

            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val modelOrigin = modelManager.getDataModelDesc(modelId);
            val model = modelManager.copyForWrite(modelOrigin);

            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(dfUpdate);
            if (model.getManagementType() == ManagementType.MODEL_BASED && model.getPartitionDesc() == null) {
                dataflowManager.fillDfManually(dataflow,
                        Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
            } else if (model.getManagementType() == ManagementType.TABLE_ORIENTED) {
                dataflowManager.fillDf(dataflow);
            }
            model.setHandledAfterBroken(false);
            modelManager.updateDataBrokenModelDesc(model);
            val eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            eventManager.postAddCuboidEvents(model.getId(), "ADMIN");
            return null;
        }, project);
    }

    private NDataModel getBrokenModel(String project, String resourcePath) {
        try {
            val resource = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource(resourcePath);
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
