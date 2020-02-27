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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.client.RestTemplate;

import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.Validation;
import lombok.val;

import static io.kyligence.kap.guava20.shaded.common.net.HttpHeaders.CONTENT_DISPOSITION;

public class NBasicController {
    private static final Logger logger = LoggerFactory.getLogger(NBasicController.class);

    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ExceptionHandler(Exception.class)
    @ResponseBody
    ErrorResponse handleError(HttpServletRequest req, Exception ex) {
        logger.error("", ex);

        Message msg = MsgPicker.getMsg();
        Throwable cause = ex;
        while (cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }

        return new ErrorResponse(req.getRequestURL().toString(), cause);
    }

    @ResponseStatus(HttpStatus.FORBIDDEN)
    @ExceptionHandler(ForbiddenException.class)
    @ResponseBody
    ErrorResponse handleForbidden(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler(NotFoundException.class)
    @ResponseBody
    ErrorResponse handleNotFound(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(BadRequestException.class)
    @ResponseBody
    ErrorResponse handleBadRequest(HttpServletRequest req, Exception ex) {
        logger.error("", ex);
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    ErrorResponse handleInvalidArgument(HttpServletRequest request, MethodArgumentNotValidException ex) {
        val response = new ErrorResponse(request.getRequestURL().toString(), ex);
        val target = ex.getBindingResult().getTarget();
        if (target instanceof Validation) {
            response.setMsg(((Validation) target).getErrorMessage(ex.getBindingResult().getFieldErrors()));
        } else {
            response.setMsg(ex.getBindingResult().getFieldErrors().stream()
                    .map(e -> e.getField() + ":" + e.getDefaultMessage()).collect(Collectors.joining(",")));
        }

        return response;
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(UnauthorizedException.class)
    @ResponseBody
    ErrorResponse handleUnauthorized(HttpServletRequest req, Exception ex) {
        return new ErrorResponse(req.getRequestURL().toString(), ex);
    }

    protected void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
            throw new BadRequestException(fieldName + " is required");
        }
    }

    protected void setDownloadResponse(File file, String fileName, final HttpServletResponse response) {
        try (InputStream fileInputStream = new FileInputStream(file);
                OutputStream output = response.getOutputStream()) {
            response.reset();
            response.setContentType("application/octet-stream");
            response.setContentLength((int) (file.length()));
            response.setHeader(CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            IOUtils.copyLarge(fileInputStream, output);
            output.flush();
        } catch (IOException e) {
            throw new InternalErrorException("Failed to download file: " + e.getMessage(), e);
        }
    }

    protected void setDownloadResponse(String downloadFile, final HttpServletResponse response) {
        File file = new File(downloadFile);
        setDownloadResponse(file, file.getName(), response);
    }

    public boolean isAdmin() {
        boolean isAdmin = false;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null) {
            for (GrantedAuthority auth : authentication.getAuthorities()) {
                if (auth.getAuthority().equals(Constant.ROLE_ADMIN)) {
                    isAdmin = true;
                    break;
                }
            }
        }
        return isAdmin;
    }

    public HashMap<String, Object> getDataResponse(String name, List<?> result, int offset, int limit) {
        HashMap<String, Object> data = new HashMap<>();
        data.put(name, PagingUtil.cutPage(result, offset, limit));
        data.put("size", result.size());
        return data;
    }

    public void checkProjectName(String project) {
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(project)) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }
    }

    public void checkJobStatus(String jobStatus) {
        Message msg = MsgPicker.getMsg();
        if (!StringUtils.isBlank(jobStatus) && Objects.isNull(JobStatusEnum.getByName(jobStatus))) {
            throw new BadRequestException(String.format(msg.getILLEGAL_JOB_STATE(), jobStatus));
        }
    }

    public void checkJobStatus(List<String> jobStatuses) {
        if (CollectionUtils.isEmpty(jobStatuses)) {
            return;
        }
        jobStatuses.forEach(this::checkJobStatus);
    }

    public void checkId(String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            throw new BadRequestException("Id cannot be empty");
        }
    }

    public void validateRange(String start, String end) {
        validateRange(Long.parseLong(start), Long.parseLong(end));
    }

    private void validateRange(long start, long end) {
        if (start < 0 || end < 0) {
            throw new BadRequestException("Start or end of range must be greater than 0!");
        }
        if (start >= end) {
            throw new BadRequestException("End of range must be greater than start!");
        }
    }

    public void validateDataRange(String start, String end) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
            return;
        }

        if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
            long startLong = 0;
            long endLong = 0;
            try {
                startLong = Long.parseLong(start);
                endLong = Long.parseLong(end);
            } catch (Exception e) {
                throw new BadRequestException(
                        "No valid value for 'start' or 'end'. Only support timestamp type, unit: ms.");
            }

            if (startLong < 0)
                throw new BadRequestException("Start of range must be greater than 0!");

            if (endLong < 0)
                throw new BadRequestException("End of range must be greater than 0!");

            if (startLong >= endLong)
                throw new BadRequestException("End of range must be greater than start!");

        } else {
            throw new BadRequestException("Start and end must exist or not at the same time!");
        }
    }

    public void checkArgsAndValidateRangeForBatchLoad(List<DateRangeRequest> requests) {
        for (DateRangeRequest request : requests) {
            checkProjectName(request.getProject());
            checkRequiredArg("table", request.getTable());
            validateRange(request.getStart(), request.getEnd());
        }
    }

    private ResponseEntity<byte[]> getHttpResponse(final HttpServletRequest request, String url) throws Exception {
        val body = IOUtils.toByteArray(request.getInputStream());
        HttpHeaders headers = new HttpHeaders();
        Collections.list(request.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(request.getHeaders(k))));
        //remove gzip
        headers.remove("accept-encoding");
        return restTemplate.exchange(url, HttpMethod.valueOf(request.getMethod()), new HttpEntity<>(body, headers),
                byte[].class);
    }

    public <T> EnvelopeResponse<T> generateTaskForRemoteHost(final HttpServletRequest request, String url)
            throws Exception {
        val response = getHttpResponse(request, url);
        return JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void downloadFromRemoteHost(final HttpServletRequest request, String url,
            HttpServletResponse servletResponse) throws Exception {
        val response = getHttpResponse(request, url);
        EnvelopeResponse exception = null;
        try {
            exception = JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
        } catch (IOException e) {
        }
        if (exception != null && StringUtils.isNotEmpty(exception.getMsg())) {
            throw new RuntimeException(exception.getMsg());
        }
        InputStream in = new ByteArrayInputStream(response.getBody());
        OutputStream out = null;
        try {
            servletResponse.reset();
            servletResponse.setContentLength(response.getBody().length);
            servletResponse.setContentType("application/octet-stream");
            servletResponse.setHeader(CONTENT_DISPOSITION, response.getHeaders().get(CONTENT_DISPOSITION).get(0));
            out = servletResponse.getOutputStream();
            IOUtils.copy(in, out);
            out.flush();
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
    }

}
