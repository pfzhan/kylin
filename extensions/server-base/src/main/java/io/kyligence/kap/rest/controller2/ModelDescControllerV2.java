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
import java.util.HashMap;

import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.KapModel;
import io.kyligence.kap.rest.response.KapModelResponse;

/**
 * @author jiazhong
 * 
 */
@Controller
@RequestMapping(value = "/model_desc")
public class ModelDescControllerV2 extends BasicController {

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    /**
     * Get detail information of the "Model ID"
     *
     * @param modelName
     *            Model ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{projectName}/{modelName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelV2(@PathVariable String projectName, @PathVariable String modelName) throws IOException {
        Message msg = MsgPicker.getMsg();

        KapModel model = (KapModel) modelService.getModel(modelName, projectName);
        Draft draft = modelService.getModelDraft(modelName, projectName);

        if (model == null && draft == null)
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));

        // figure out project
        String project = null;
        if (model != null) {
            project = projectService.getProjectOfModel(modelName);
        } else {
            project = draft.getProject();
        }

        // result
        HashMap<String, KapModelResponse> result = new HashMap<String, KapModelResponse>();
        if (model != null) {
            Preconditions.checkState(!model.isDraft());
            KapModelResponse r = new KapModelResponse(model);
            r.setProject(project);
            result.put("model", r);
        }
        if (draft != null && draft.getEntity() instanceof KapModel) {
            KapModel dm = (KapModel) draft.getEntity();
            Preconditions.checkState(dm.isDraft());
            KapModelResponse r = new KapModelResponse(dm);
            r.setProject(project);
            result.put("draft", r);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

}
