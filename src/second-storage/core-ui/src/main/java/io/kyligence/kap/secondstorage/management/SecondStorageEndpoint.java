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

package io.kyligence.kap.secondstorage.management;

import com.google.common.base.Preconditions;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_JSON})
@Slf4j
@ConditionalOnProperty({"kylin.second-storage.class"})
public class SecondStorageEndpoint extends NBasicController {
    private static final String MODEL_ARG_NAME = "model";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("secondStorageService")
    private SecondStorageService secondStorageService;

    public SecondStorageEndpoint setSecondStorageService(SecondStorageService secondStorageService) {
        this.secondStorageService = secondStorageService;
        return this;
    }

    public SecondStorageEndpoint setModelService(final ModelService modelService) {
        this.modelService = modelService;
        return this;
    }

    @ApiOperation(value = "loadSegments")
    @PostMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> loadStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModel());
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        return internalLoadIntoStorage(request);
    }

    @ApiOperation(value = "cleanSegments")
    @DeleteMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse cleanStorage(StorageRequest request,
                                         @RequestParam(name="segment_ids") List<String> segmentIds) {
        request.setSegmentIds(segmentIds);
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModel());
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        secondStorageService.triggerSegmentsClean(request.getProject(), request.getModel(), new HashSet<>(request.getSegmentIds()));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "enableModel")
    @PostMapping(value = "/model/state")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> enableStorage(@RequestBody ModelEnableRequest modelEnableRequest) {
        checkProjectName(modelEnableRequest.getProject());
        checkRequiredArg(MODEL_ARG_NAME, modelEnableRequest.getModel());
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), modelEnableRequest.getProject());
        val model = modelManager.getDataModelDesc(modelEnableRequest.getModel());
        checkModel(modelEnableRequest.getProject(), model.getAlias());
        val jobInfo = secondStorageService.changeModelSecondStorageState(modelEnableRequest.getProject(),
                modelEnableRequest.getModel(), modelEnableRequest.isEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "enableProject")
    @PostMapping(value = "/project/state")
    public EnvelopeResponse<JobInfoResponse> enableProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        checkProjectName(projectEnableRequest.getProject());
        val jobInfo = secondStorageService.changeProjectSecondStorageState(projectEnableRequest.getProject(),
                projectEnableRequest.getNewNodes(),
                projectEnableRequest.isEnabled());
        JobInfoResponse jobInfoResponse = new JobInfoResponse();
        jobInfoResponse.setJobs(Collections.singletonList(jobInfo.orElse(null)));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, jobInfoResponse, "");
    }

    @ApiOperation(value = "disableProjectStorageValidation")
    @PostMapping(value="/project/state/validation")
    public EnvelopeResponse<List<String>> validateProjectStorage(@RequestBody ProjectEnableRequest projectEnableRequest) {
        checkProjectName(projectEnableRequest.getProject());
        List<String> models = secondStorageService.getAllSecondStorageModel(projectEnableRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, models, "");
    }

    @ApiOperation(value = "listSecondStorageNodes")
    @GetMapping(value = "/nodes")
    @ResponseBody
    public EnvelopeResponse<List<NodeData>> listNodes() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, secondStorageService.listAvailableNodes(), "");
    }

    private EnvelopeResponse<JobInfoResponse> internalLoadIntoStorage(StorageRequest request) {
        String[] segIds = modelService.convertSegmentIdWithName(request.getModel(), request.getProject(),
                request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));

        if (ArrayUtils.isEmpty(segIds)) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT());
        }

        if (!request.storageTypeSupported()) {
            throw new KylinException(INVALID_PARAMETER, "");
        }

        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(modelService.exportSegmentToSecondStorage(request.getProject(), request.getModel(), segIds));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PostMapping(value = "/recovery/model")
    public EnvelopeResponse<Void> recoverModel(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("modelName", request.getModelName());
        checkModel(request.getProject(), request.getModelName());
        importSingleModel(request.getProject(), request.getModelName());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    private void importSingleModel(String project, String modelName) {
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDescByAlias(modelName).getUuid();
        Preconditions.checkState(SecondStorageUtil.isModelEnable(project, model),
                "model %s doesn't enable tiered storage.", model);

        val dataflowManager = NDataflowManager.getInstance(config, project);
        val segIds = dataflowManager.getDataflow(model).getQueryableSegments().stream()
                .map(NDataSegment::getId).collect(Collectors.toList());
        modelService.exportSegmentToSecondStorage(project, model, segIds.toArray(new String[]{}));
    }

    @PostMapping(value = "/recovery/project")
    public EnvelopeResponse<ProjectRecoveryResponse> recoverProject(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        secondStorageService.isProjectAdmin(request.getProject());
        val config = KylinConfig.getInstanceFromEnv();
        val modelManager = NDataModelManager.getInstance(config, request.getProject());
        val dataflowManager = NDataflowManager.getInstance(config, request.getProject());
        val allModelAlias = modelManager.listAllModelAlias();
        val execManager = NExecutableManager.getInstance(config, request.getProject());
        List<String> failedModels = new ArrayList<>();
        List<String> submittedModels = new ArrayList<>();
        val validModels = allModelAlias.stream()
                .map(modelName -> modelManager.getDataModelDescByAlias(modelName).getUuid())
                .filter(modelId -> SecondStorageUtil.isModelEnable(request.getProject(), modelId))
                .filter(modelId -> {
                    val jobs = execManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning);
                    if (!jobs.isEmpty()) {
                        failedModels.add(modelManager.getDataModelDesc(modelId).getAlias());
                    }
                    val dataflow = dataflowManager.getDataflow(modelId);
                    return jobs.isEmpty() && !dataflow.getSegments().isEmpty();
                })
                .map(modelId -> modelManager.getDataModelDesc(modelId).getAlias())
                .collect(Collectors.toList());
        for (val modelName : validModels) {
            try {
                importSingleModel(request.getProject(), modelName);
                submittedModels.add(modelName);
            } catch (Exception e) {
                failedModels.add(modelName);
                log.error("model {} recover failed", modelName, e);
            }
        }
        ProjectRecoveryResponse response = new ProjectRecoveryResponse();
        response.setSubmittedModels(submittedModels);
        response.setFailedModels(failedModels);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @PostMapping(value = "/config/refresh")
    public EnvelopeResponse<ProjectRecoveryResponse> refreshConf() {
        secondStorageService.refreshConf();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }
    @PostMapping(value = "/reset")
    public EnvelopeResponse resetStorage() {
        secondStorageService.resetStorage();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    private void checkModel(String project, String modelName) {
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val model = modelManager.getDataModelDescByAlias(modelName);
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NOT_EXIST,
                    "Model " + modelName + " does not exist in project " + project);
        }
    }
}
