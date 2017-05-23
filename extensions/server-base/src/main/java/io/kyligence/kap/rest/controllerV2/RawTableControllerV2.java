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

package io.kyligence.kap.rest.controllerV2;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.RawTableRequest;
import io.kyligence.kap.rest.service.RawTableServiceV2;

@Controller
@RequestMapping(value = "/rawtables")
public class RawTableControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(RawTableControllerV2.class);

    @Autowired
    @Qualifier("rawTableServiceV2")
    private RawTableServiceV2 rawTableServiceV2;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    private RawTableDesc deserializeRawTableDesc(RawTableRequest rawTableRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableDesc desc = null;
        try {
            logger.debug("Saving rawtable " + rawTableRequest.getRawTableDescData());
            desc = JsonUtil.readValue(rawTableRequest.getRawTableDescData(), RawTableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The rawtable definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_RAWTABLE_DEFINITION());
        }
        return desc;
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getRawTableDesc(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableServiceV2.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, raw.getRawTableDesc(), "");
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.DELETE }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteRaw(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableServiceV2.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }
        rawTableServiceV2.deleteRaw(raw);
    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse enableRaw(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableServiceV2.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, rawTableServiceV2.enableRaw(raw), "");
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse disableRaw(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        RawTableInstance raw = rawTableServiceV2.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw table" + cubeName + " does not exist!");
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, rawTableServiceV2.disableRaw(raw), "");
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rawCube(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName, @RequestBody CubeRequest cubeRequest) throws IOException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        String newRawName = cubeRequest.getCubeName();
        String project = cubeRequest.getProject();

        RawTableInstance raw = rawTableServiceV2.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            throw new BadRequestException(String.format(msg.getRAWTABLE_NOT_FOUND(), cubeName));
        }

        RawTableDesc rawDesc = raw.getRawTableDesc();
        RawTableDesc newRawDesc = RawTableDesc.getCopyOf(rawDesc);

        KylinConfig config = rawTableServiceV2.getConfig();
        newRawDesc.setName(newRawName);
        newRawDesc.setEngineType(config.getDefaultCubeEngine());
        newRawDesc.setStorageType(config.getDefaultStorageEngine());

        RawTableInstance newRaw;

        newRaw = rawTableServiceV2.createRawTableInstanceAndDesc(newRawName, project, newRawDesc);

        //reload to avoid shallow clone
        rawTableServiceV2.getCubeDescManager().reloadCubeDescLocal(newRawName);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, newRaw, "");
    }

    public void setRawTableService(RawTableServiceV2 rawTableServiceV2) {
        this.rawTableServiceV2 = rawTableServiceV2;
    }

    public void setJobService(JobService jobServiceV2) {
        this.jobService = jobServiceV2;
    }
}
