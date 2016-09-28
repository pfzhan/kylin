/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
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
import io.kyligence.kap.rest.service.RawTableService;

@Controller
@RequestMapping(value = "/rawtables")
public class RawTableController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(RawTableController.class);

    @Autowired
    private RawTableService rawService;

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
            rawService.createRawTableInstanceAndDesc(name, projectName, desc);
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
            RawTableInstance raw = rawService.getRawTableManager().getRawTableInstance(rawRequest.getRawTableName());

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

            desc = rawService.updateRawTableInstanceAndDesc(raw, desc, projectName, true);

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

    @RequestMapping(value = "/{rawTableName}", method = { RequestMethod.GET })
    @ResponseBody
    public RawTableDesc getRawTable(@PathVariable String rawTableName) {
        RawTableInstance raw = rawService.getRawTableManager().getRawTableInstance(rawTableName);
        if (raw == null) {
            throw new InternalErrorException("Cannot find raw " + rawTableName);
        }
        return raw.getRawTableDesc();
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteRaw(@PathVariable String cubeName) {
        RawTableInstance raw = rawService.getRawTableManager().getRawTableInstance(cubeName);
        if (null == raw) {
            throw new NotFoundException("Raw with name " + cubeName + " not found..");
        }

        try {
            rawService.deleteRaw(raw);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete raw. " + " Caused by: " + e.getMessage(), e);
        }

    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT })
    @ResponseBody
    public RawTableInstance enableRaw(@PathVariable String cubeName) {
        try {
            RawTableInstance raw = rawService.getRawTableManager().getRawTableInstance(cubeName);
            if (null == raw) {
                throw new InternalErrorException("Cannot find raw " + cubeName);
            }

            return rawService.enableRaw(raw);
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
            RawTableInstance raw = rawService.getRawTableManager().getRawTableInstance(cubeName);
            if (null == raw) {
                throw new InternalErrorException("Cannot find raw " + cubeName);
            }

            return rawService.disableRaw(raw);
        } catch (Exception e) {
            String message = "Failed to enable raw: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    private void updateRequest(RawTableRequest request, boolean success, String message) {
        request.setRawTableDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setCubeService(RawTableService rawService) {
        this.rawService = rawService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }
}
