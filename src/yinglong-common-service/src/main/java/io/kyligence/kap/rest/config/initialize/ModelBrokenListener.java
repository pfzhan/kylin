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
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.ref.OptRecManagerV2;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.rest.delegate.JobMetadataBaseInvoker;
import io.kyligence.kap.rest.delegate.JobMetadataRequest;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelBrokenListener {

    private boolean needHandleModelBroken(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        return model != null && model.isBroken() && !model.isHandledAfterBroken();
    }

    @Subscribe
    public void onModelBroken(NDataModel.ModelBrokenEvent event) {
        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelBroken(project, modelId)) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (!needHandleModelBroken(project, modelId)) {
                return null;
            }

            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);

            val model = getBrokenModel(project, NDataModel.concatResourcePath(modelId, project));

            val dataflowManager = NDataflowManager.getInstance(config, project);
            val indexPlanManager = NIndexPlanManager.getInstance(config, project);

            if (config.getSmartModeBrokenModelDeleteEnabled()) {
                dataflowManager.dropDataflow(model.getId());
                indexPlanManager.dropIndexPlan(model.getId());
                modelManager.dropModel(model);
                return null;
            }
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setStatus(RealizationStatusEnum.BROKEN);
            if (model.getBrokenReason() == NDataModel.BrokenReason.SCHEMA) {
                dfUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
            }
            dataflowManager.updateDataflow(dfUpdate);
            model.setHandledAfterBroken(true);
            model.setRecommendationsCount(0);
            modelManager.updateDataBrokenModelDesc(model);

            OptRecManagerV2 optRecManagerV2 = OptRecManagerV2.getInstance(project);
            optRecManagerV2.discardAll(model.getId());
            return null;
        }, project);
    }

    private boolean needHandleModelRepair(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        return model != null && !model.isBroken() && model.isHandledAfterBroken();
    }

    @Subscribe
    public void onModelRepair(NDataModel.ModelRepairEvent event) {

        val project = event.getProject();
        val modelId = event.getSubject();

        if (!needHandleModelRepair(project, modelId)) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {

            if (!needHandleModelRepair(project, modelId)) {
                return null;
            }
            val config = KylinConfig.getInstanceFromEnv();
            val modelManager = NDataModelManager.getInstance(config, project);
            val modelOrigin = modelManager.getDataModelDesc(modelId);
            val model = modelManager.copyForWrite(modelOrigin);

            val dataflowManager = NDataflowManager.getInstance(config, project);
            val dataflow = dataflowManager.getDataflow(modelId);
            val dfUpdate = new NDataflowUpdate(dataflow.getId());

            dfUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            //if scd2 turn off , model should be offline
            if (dataflow.getLastStatus() != null && !checkSCD2Disabled(project, modelId)) {
                dfUpdate.setStatus(dataflow.getLastStatus());
            }
            dataflowManager.updateDataflow(dfUpdate);
            if (CollectionUtils.isEmpty(dataflow.getSegments())) {
                if (model.getManagementType() == ManagementType.MODEL_BASED && model.getPartitionDesc() == null) {
                    dataflowManager.fillDfManually(dataflow,
                            Lists.newArrayList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
                } else if (model.getManagementType() == ManagementType.TABLE_ORIENTED) {
                    dataflowManager.fillDf(dataflow);
                }
                final JobParam jobParam = new JobParam(model.getId(), "ADMIN");
                jobParam.setProject(project);
                val sourceUsageManager = SourceUsageManager.getInstance(config);
                sourceUsageManager.licenseCheckWrap(project,
                        () -> JobMetadataBaseInvoker.getInstance().addIndexJob(new JobMetadataRequest(jobParam)));
            }
            model.setHandledAfterBroken(false);
            modelManager.updateDataBrokenModelDesc(model);

            return null;
        }, project);
    }

    private boolean checkSCD2Disabled(String project, String modelId) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(modelId);

        boolean isSCD2 = SCD2CondChecker.INSTANCE.isScd2Model(model);

        return !NProjectManager.getInstance(config).getProject(project).getConfig().isQueryNonEquiJoinModelEnabled()
                && isSCD2;
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
