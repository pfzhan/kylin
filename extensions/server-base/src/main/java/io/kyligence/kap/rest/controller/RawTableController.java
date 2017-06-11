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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.JobService;
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

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.service.RawTableService;

@Controller
@RequestMapping(value = "/rawtables")
public class RawTableController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(RawTableController.class);

    @Autowired
    @Qualifier("rawTableService")
    private RawTableService rawTableService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getRawTableDesc(@PathVariable String cubeName) {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, raw.getRawTableDesc(), "");
    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse enableRaw(@PathVariable String cubeName) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, rawTableService.enableRaw(raw), "");
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse disableRaw(@PathVariable String cubeName) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, rawTableService.disableRaw(raw), "");
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rawCube(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest)
            throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String newRawName = cubeRequest.getCubeName();
        String project = cubeRequest.getProject();

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        RawTableDesc rawDesc = raw.getRawTableDesc();
        RawTableDesc newRawDesc = RawTableDesc.getCopyOf(rawDesc);

        KylinConfig config = rawTableService.getConfig();
        newRawDesc.setName(newRawName);
        newRawDesc.setEngineType(config.getDefaultCubeEngine());
        newRawDesc.setStorageType(config.getDefaultStorageEngine());

        RawTableInstance newRaw;

        newRaw = rawTableService.createRawTableInstanceAndDesc(newRawName, project, newRawDesc);

        //reload to avoid shallow clone
        rawTableService.getCubeDescManager().reloadCubeDescLocal(newRawName);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, newRaw, "");
    }

    public void setRawTableService(RawTableService rawTableService) {
        this.rawTableService = rawTableService;
    }

    public void setJobService(JobService jobServiceV2) {
        this.jobService = jobServiceV2;
    }
}
