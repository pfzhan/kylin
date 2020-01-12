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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.CSVRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.service.FileSourceService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/source", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class FileSourceController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(FileSourceController.class);

    @Autowired
    @Qualifier("fileSourceService")
    private FileSourceService fileSourceService;

    @ApiOperation(value = "saveCsv (update)", notes = "Update URL: /save")
    @PostMapping(value = "/csv")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> saveCsv(@RequestParam(value = "mode") String mode,
            @RequestBody CSVRequest csvRequest) throws Exception {
        checkProjectName(csvRequest.getProject());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, fileSourceService.saveCSV(mode, csvRequest), "");
    }

    @PostMapping(value = "/verify")
    @ResponseBody
    public EnvelopeResponse<Boolean> verifyCredential(@RequestBody CSVRequest csvRequest) {
        try {
            fileSourceService.verifyCredential(csvRequest);
        } catch (Exception e) {
            logger.info("ICredential Verify failed.", e);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, false, e.getMessage());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
    }

    @PostMapping(value = "/csv/samples")
    @ResponseBody
    public EnvelopeResponse<String[][]> csvSamples(@RequestBody CSVRequest csvRequest) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, fileSourceService.csvSamples(csvRequest), "");
    }

    @PostMapping(value = "/csv/schema")
    @ResponseBody
    public EnvelopeResponse<List<String>> csvSchema(@RequestBody CSVRequest csvRequest) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, fileSourceService.csvSchema(csvRequest), "");
    }

    @PostMapping(value = "/validate")
    @ResponseBody
    public EnvelopeResponse<Boolean> validateSql(@RequestBody CSVRequest csvRequest) {
        try {
            fileSourceService.validateSql(csvRequest.getDdl());
        } catch (Exception e) {
            logger.info("validate sql failed.", e);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, false, e.getMessage());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
    }
}