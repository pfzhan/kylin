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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.draft.DraftManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.DataModelDescResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * ModelController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/models")
public class ModelControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ModelControllerV2.class);

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelsPaging(@RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "exactMatch", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        List<DataModelDescResponse> response = new ArrayList<DataModelDescResponse>();

        // official models
        for (DataModelDesc m : modelService.listAllModels(modelName, projectName, exactMatch)) {
            Preconditions.checkState(!m.isDraft());

            DataModelDescResponse r = new DataModelDescResponse(m);
            r.setProject(projectService.getProjectOfModel(m.getName()));
            response.add(r);
        }

        // draft models
        for (Draft d : modelService.listModelDrafts(modelName, projectName)) {
            DataModelDesc m = (DataModelDesc) d.getEntity();
            Preconditions.checkState(m.isDraft());

            if (contains(response, m.getName()) == false) {
                DataModelDescResponse r = new DataModelDescResponse(m);
                r.setProject(d.getProject());
                response.add(r);
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;
        int size = response.size();

        if (size <= offset) {
            offset = size;
            limit = 0;
        }

        if ((size - offset) < limit) {
            limit = size - offset;
        }
        HashMap<String, Object> data = new HashMap<String, Object>();
        data.put("models", response.subList(offset, offset + limit));
        data.put("size", size);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private boolean contains(List<DataModelDescResponse> response, String name) {
        for (DataModelDescResponse m : response) {
            if (m.getName().equals(name))
                return true;
        }
        return false;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDescV2(@RequestBody ModelRequest modelRequest) throws IOException {
        DraftManager draftMgr = modelService.getDraftManager();

        DataModelDesc modelDesc = deserializeDataModelDescV2(modelRequest);
        modelService.primaryCheck(modelDesc);

        String project = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : modelRequest.getProject();

        // don't use checkpoint/rollback, the following update is the only change that must succeed

        // save/update model
        modelDesc = modelService.updateModelToResourceStore(modelDesc, project);

        // remove any previous draft
        draftMgr.delete(modelDesc.getUuid());

        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", modelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/validness", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse checkModel(@RequestBody ModelRequest modelRequest) throws IOException {
        Preconditions.checkNotNull(modelRequest.getProject());
        Preconditions.checkNotNull(modelRequest.getModelDescData());

        DataModelDesc modelDesc = deserializeDataModelDescV2(modelRequest);
        modelService.primaryCheck(modelDesc);
        modelService.checkCCExpression(modelDesc, modelRequest.getProject());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDescDraftV2(@RequestBody ModelRequest modelRequest) throws IOException {
        DraftManager draftMgr = modelService.getDraftManager();

        DataModelDesc modelDesc = deserializeDataModelDescV2(modelRequest);
        modelService.primaryCheck(modelDesc);

        String project = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : modelRequest.getProject();

        if (modelDesc.getUuid() == null)
            modelDesc.updateRandomUuid();
        modelDesc.setDraft(true);

        draftMgr.save(project, modelDesc.getUuid(), modelDesc);

        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", modelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{projectName}/{modelName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteModelV2(@PathVariable String projectName, @PathVariable String modelName) throws IOException {
        Message msg = MsgPicker.getMsg();

        DataModelDesc model = modelService.getModel(modelName, projectName);
        Draft draft = modelService.getModelDraft(modelName, projectName);
        if (null == model && null == draft)
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));

        if (model != null)
            modelService.dropModel(model);

        if (draft != null)
            modelService.getDraftManager().delete(draft.getUuid());
    }

    @RequestMapping(value = "/{modelName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneModelV2(@PathVariable String modelName, @RequestBody ModelRequest modelRequest)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        String project = modelRequest.getProject();
        MetadataManager metaManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc modelDesc = metaManager.getDataModelDesc(modelName);
        String newModelName = modelRequest.getModelName();

        if (StringUtils.isEmpty(project)) {
            logger.info("Project name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (modelDesc == null || StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }

        if (StringUtils.isEmpty(newModelName)) {
            logger.info("New model name is empty.");
            throw new BadRequestException(msg.getEMPTY_NEW_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(newModelName, ModelService.VALID_MODELNAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", newModelName);
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newModelName));
        }

        DataModelDesc newModelDesc = DataModelDesc.getCopyOf(modelDesc);
        newModelDesc.setName(newModelName);

        newModelDesc = modelService.createModelDesc(project, newModelDesc);

        //reload avoid shallow
        metaManager.reloadDataModelDescAt(DataModelDesc.concatResourcePath(newModelName));

        String descData = JsonUtil.writeValueAsIndentString(newModelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", newModelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private DataModelDesc deserializeDataModelDescV2(ModelRequest modelRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        DataModelDesc desc = null;
        try {
            logger.debug("deserialize MODEL " + modelRequest.getModelDescData());
            desc = JsonUtil.readValue(modelRequest.getModelDescData(), DataModelDesc.class);
        } catch (JsonParseException e) {
            logger.error("The data model definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data model definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        }
        return desc;
    }

    @RequestMapping(value = "/{modelName}/{projectName}/usedCols", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getUsedColsV2(@PathVariable String projectName, @PathVariable String modelName) {

        Map<String, Set<String>> data = new HashMap<>();

        for (Map.Entry<TblColRef, Set<CubeInstance>> entry : modelService.getUsedDimCols(modelName, projectName)
                .entrySet()) {
            populateUsedColResponse(entry.getKey(), entry.getValue(), data);
        }

        for (Map.Entry<TblColRef, Set<CubeInstance>> entry : modelService.getUsedNonDimCols(modelName, projectName)
                .entrySet()) {
            populateUsedColResponse(entry.getKey(), entry.getValue(), data);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private void populateUsedColResponse(TblColRef tblColRef, Set<CubeInstance> cubeInstances,
            Map<String, Set<String>> ret) {
        String columnIdentity = tblColRef.getIdentity();
        if (!ret.containsKey(columnIdentity)) {
            ret.put(columnIdentity, Sets.<String> newHashSet());
        }

        for (CubeInstance cubeInstance : cubeInstances) {
            ret.get(columnIdentity).add(cubeInstance.getCanonicalName());
        }
    }

    public void setModelService(ModelService modelService) {
        this.modelService = modelService;
    }

}
