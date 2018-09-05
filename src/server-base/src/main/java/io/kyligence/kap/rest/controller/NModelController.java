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

package io.kyligence.kap.rest.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.ProjectService;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.ModifiedOrder;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

@Controller
@RequestMapping(value = "/models")
public class NModelController extends NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NModelController.class);
    private static final Message msg = MsgPicker.getMsg();
    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("aclEvaluate")
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModels(@RequestParam(value = "model", required = false) String modelName,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {
        checkProjectName(project);
        List<NDataModel> models = new ArrayList<NDataModel>();
        if (StringUtils.isEmpty(table)) {
            for (NDataModel modelDesc : modelService.getModels(modelName, project, exactMatch)) {
                Preconditions.checkState(!modelDesc.isDraft());
                models.add(modelDesc);
            }
        } else {
            models.addAll(modelService.getRelateModels(project, table));
        }
        //todo draft
        Collections.sort(models, new ModifiedOrder());
        HashMap<String, Object> modelResponse = getDataResponse("models", models, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, modelResponse, "");
    }

    @RequestMapping(value = "/segments", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSegments(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "startTime", required = false, defaultValue = "1") Long startTime,
            @RequestParam(value = "endTime", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) Long endTime) {
        checkProjectName(project);
        List<NDataSegment> segementsResult = new ArrayList<NDataSegment>();
        Segments<NDataSegment> segments = modelService.getSegments(modelName, project, startTime, endTime);
        HashMap<String, Object> response = getDataResponse("segments", segments, offset, limit);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/agg_indexs", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getAggIndexs(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        List<NCuboidDesc> aggIndexs = modelService.getAggIndexs(modelName, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, aggIndexs, "");
    }

    @RequestMapping(value = "/cuboids", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getCuboids(@RequestParam(value = "id", required = true) Long id,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "model", required = true) String modelName) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        NCuboidDesc cuboidDesc = modelService.getCuboidById(modelName, project, id);
        if (cuboidDesc == null) {
            throw new BadRequestException("Can not find this cuboid " + id);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cuboidDesc, "");
    }

    @RequestMapping(value = "/table_indexs", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getTableIndexs(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        List<NCuboidDesc> tableIndexs = modelService.getTableIndexs(modelName, project);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableIndexs, "");
    }

    @RequestMapping(value = "/json", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getModelJson(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {

        checkProjectName(project);
        if (StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        try {
            String json = modelService.getModelJson(modelName, project);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, json, "");
        } catch (JsonProcessingException e) {
            throw new BadRequestException("can not get model json " + e);
        }

    }

    @RequestMapping(value = "/relations", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getModelRelations(@RequestParam(value = "model", required = true) String modelName,
            @RequestParam(value = "project", required = true) String project) {

        checkProjectName(project);
        if (StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }

        List<NForestSpanningTree> relations = modelService.getModelRelations(modelName, project);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, relations, "");
    }

}
