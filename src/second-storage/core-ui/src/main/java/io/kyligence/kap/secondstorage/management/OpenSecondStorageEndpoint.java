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

import com.google.common.collect.Lists;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@RestController
@RequestMapping(value = "/api/storage", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
@Slf4j
@ConditionalOnProperty({"kylin.second-storage.class"})
public class OpenSecondStorageEndpoint extends NBasicController {
    private static final String MODEL_ARG_NAME = "model_name";

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("secondStorageEndpoint")
    private SecondStorageEndpoint secondStorageEndpoint;

    @Autowired
    @Qualifier("secondStorageService")
    private SecondStorageService secondStorageService;

    public OpenSecondStorageEndpoint setModelService(final ModelService modelService) {
        this.modelService = modelService;
        return this;
    }

    public OpenSecondStorageEndpoint setSecondStorageService(final SecondStorageService secondStorageService) {
        this.secondStorageService = secondStorageService;
        return this;
    }

    public OpenSecondStorageEndpoint setSecondStorageEndpoint(final SecondStorageEndpoint secondStorageEndpoint) {
        this.secondStorageEndpoint = secondStorageEndpoint;
        return this;
    }

    @ApiOperation(value = "loadSegments")
    @PostMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> loadStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        String modelId = modelService.getModelDesc(request.getModelName(), request.getProject()).getUuid();
        request.setModel(modelId);

        checkSecondStorageEnabled(request);
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        return secondStorageEndpoint.loadStorage(request);
    }

    @ApiOperation(value = "cleanSegments")
    @DeleteMapping(value = "/segments")
    @ResponseBody
    public EnvelopeResponse<Void> cleanStorage(@RequestBody StorageRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ARG_NAME, request.getModelName());
        String modelId = modelService.getModelDesc(request.getModelName(), request.getProject()).getUuid();
        request.setModel(modelId);

        checkSecondStorageEnabled(request);
        checkSegmentParms(request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        List<String> segIds = convertSegmentIdWithName(request);
        request.setSegmentNames(Lists.newArrayList());
        return secondStorageEndpoint.cleanStorage(request, segIds);
    }

    protected List<String> convertSegmentIdWithName(StorageRequest request) {
        String[] segIds = modelService.convertSegmentIdWithName(request.getModel(), request.getProject(),
                request.getSegmentIds().toArray(new String[0]),
                request.getSegmentNames().toArray(new String[0]));
        modelService.checkSegmentsExistById(request.getModel(), request.getProject(), segIds);

        if (segIds == null)
            throw new KylinException(SEGMENT_EMPTY_PARAMETER);

        return Lists.newArrayList(segIds);
    }

    private void checkSecondStorageEnabled(StorageRequest request) {
        if (!SecondStorageUtil.isProjectEnable(request.getProject())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_PROJECT_ENABLED(), request.getProject()));

        }

        if (!SecondStorageUtil.isModelEnable(request.getProject(), request.getModel())) {
            throw new KylinException(SECOND_STORAGE_PROJECT_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_MODEL_ENABLED(), request.getModelName()));
        }
    }

    @PostMapping(value = "/recovery/model", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<Void> recoverModel(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg("modelName", request.getModelName());
        secondStorageEndpoint.checkModel(request.getProject(), request.getModelName());
        secondStorageService.importSingleModel(request.getProject(), request.getModelName());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/recovery/project", produces = {HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON})
    public EnvelopeResponse<ProjectRecoveryResponse> recoverProject(@RequestBody RecoverRequest request) {
        checkProjectName(request.getProject());
        secondStorageService.isProjectAdmin(request.getProject());
        ProjectLoadResponse response = secondStorageService.projectLoadData(Arrays.asList(request.getProject()));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response.getLoads().get(0), "");
    }
}
