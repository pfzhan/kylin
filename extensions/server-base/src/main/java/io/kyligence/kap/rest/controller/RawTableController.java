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
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.service.JobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.request.RawTableRequest;
import io.kyligence.kap.rest.service.RawTableService;

@Controller
@RequestMapping(value = "/rawtables")
public class RawTableController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(RawTableController.class);

    @Autowired
    private RawTableService rawTableService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody
    public RawTableRequest saveRawTableDesc(@RequestBody RawTableRequest rawRequest) {

        RawTableDesc desc = deserializeRawTableDesc(rawRequest);
        if (desc == null) {
            rawRequest.setMessage("RawTableDesc is null.");
            return rawRequest;
        }
        String name = RawTableService.getRawTableNameFromDesc(desc.getName());
        if (StringUtils.isEmpty(name)) {
            logger.info("RawTable name should not be empty.");
            throw new BadRequestException("RawTable name should not be empty.");
        }

        try {
            desc.setUuid(UUID.randomUUID().toString());
            String projectName = (null == rawRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : rawRequest.getProject();
            rawTableService.createRawTableInstanceAndDesc(name, projectName, desc);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        rawRequest.setUuid(desc.getUuid());
        rawRequest.setSuccessful(true);
        return rawRequest;
    }

    private RawTableDesc deserializeRawTableDesc(RawTableRequest rawTableRequest) {
        RawTableDesc desc = null;
        try {
            logger.debug("Saving rawtable " + rawTableRequest.getRawTableDescData());
            desc = JsonUtil.readValue(rawTableRequest.getRawTableDescData(), RawTableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The rawtable definition is not valid.", e);
            updateRequest(rawTableRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The rawtable definition is not valid.", e);
            updateRequest(rawTableRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
    public RawTableRequest updateRawTableDesc(@RequestBody RawTableRequest rawRequest) throws JsonProcessingException {

        RawTableDesc desc = deserializeRawTableDesc(rawRequest);
        if (desc == null) {
            return rawRequest;
        }

        String projectName = (null == rawRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : rawRequest.getProject();
        try {
            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(rawRequest.getRawTableName());

            if (raw == null) {
                String error = "The raw named " + rawRequest.getRawTableName() + " does not exist ";
                updateRequest(rawRequest, false, error);
                return rawRequest;
            }

            //cube renaming is not allowed
            if (!raw.getRawTableDesc().getName().equalsIgnoreCase(desc.getName())) {
                String error = "Raw Desc renaming is not allowed: desc.getName(): " + desc.getName() + ", rawRequest.getCubeName(): " + rawRequest.getRawTableName();
                updateRequest(rawRequest, false, error);
                return rawRequest;
            }

            desc = rawTableService.updateRawTableInstanceAndDesc(raw, desc, projectName, true);

        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this cube.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        String descData = JsonUtil.writeValueAsIndentString(desc);
        rawRequest.setRawTableDescData(descData);
        rawRequest.setSuccessful(true);
        return rawRequest;
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET })
    @ResponseBody
    public RawTableDesc getRawTableDesc(@PathVariable String cubeName) {
        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            logger.info("raw " + cubeName + " does not exist!");
            return null;
        }
        return raw.getRawTableDesc();
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteRaw(@PathVariable String cubeName) {
        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            logger.info("raw " + cubeName + " does not exist!");
            return;
        }
        try {
            rawTableService.deleteRaw(raw);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete raw. " + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT })
    @ResponseBody
    public RawTableInstance enableRaw(@PathVariable String cubeName) {
        try {
            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (null == raw) {
                logger.info("raw " + cubeName + " does not exist!");
                return null;
            }

            return rawTableService.enableRaw(raw);
        } catch (Exception e) {
            String message = "Failed to enable raw: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT })
    @ResponseBody
    public RawTableInstance disableRaw(@PathVariable String cubeName) {
        try {
            RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
            if (null == raw) {
                logger.info("raw " + cubeName + " does not exist!");
                return null;
            }
            return rawTableService.disableRaw(raw);
        } catch (Exception e) {
            String message = "Failed to enable raw: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT })
    @ResponseBody
    public RawTableInstance rawCube(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest) {
        String newRawName = cubeRequest.getCubeName();
        String project = cubeRequest.getProject();

        RawTableInstance raw = rawTableService.getRawTableManager().getRawTableInstance(cubeName);
        if (raw == null) {
            return null;
        }

        RawTableDesc rawDesc = raw.getRawTableDesc();
        RawTableDesc newRawDesc = rawDesc.getCopyOf(rawDesc);

        KylinConfig config = rawTableService.getConfig();
        newRawDesc.setName(newRawName);
        newRawDesc.setEngineType(config.getDefaultCubeEngine());
        newRawDesc.setStorageType(config.getDefaultStorageEngine());

        RawTableInstance newRaw;
        try {
            newRaw = rawTableService.createRawTableInstanceAndDesc(newRawName, project, newRawDesc);

            //reload to avoid shallow clone
            rawTableService.getCubeDescManager().reloadCubeDescLocal(newRawName);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to clone cube ", e);
        }

        return newRaw;

    }

    private void updateRequest(RawTableRequest request, boolean success, String message) {
        request.setRawTableDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setRawTableService(RawTableService rawTableService) {
        this.rawTableService = rawTableService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }
}
