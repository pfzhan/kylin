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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.KapSuggestionService;

@Controller
@RequestMapping(value = "/smart")
public class KapSuggestionController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KapSuggestionController.class);

    @Autowired
    @Qualifier("kapSuggestionService")
    private KapSuggestionService kapSuggestionService;

    @RequestMapping(value = "{modelName}/{cubeName}/collect_sql", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse saveSampleSqls(@PathVariable String modelName, @PathVariable String cubeName,
            @RequestBody List<String> sqls) throws Exception {

        kapSuggestionService.saveSampleSqls(modelName, cubeName, sqls);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "{cubeName}/get_sql", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSampleSqls(@PathVariable String cubeName) throws Exception {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kapSuggestionService.getSampleSqls(cubeName), "");
    }

    @RequestMapping(value = "{modelName}/{cubeName}/dimension_measure", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse proposeDimAndMeasures(@PathVariable String modelName, @PathVariable String cubeName)
            throws IOException {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                kapSuggestionService.proposeDimAndMeasures(cubeName, modelName), "");
    }

    @RequestMapping(value = "aggregation_groups", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse proposeAggGroups(@RequestBody CubeRequest cubeRequest) throws IOException {

        CubeDesc cubeDesc = deserializeCubeDesc(cubeRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kapSuggestionService.proposeAggGroups(cubeDesc), "");
    }

    private CubeDesc deserializeCubeDesc(CubeRequest cubeRequest) throws IOException {
        logger.debug("Saving cube " + cubeRequest.getCubeDescData());
        CubeDesc desc = JsonUtil.readValue(cubeRequest.getCubeDescData(), CubeDesc.class);
        return desc;
    }
}