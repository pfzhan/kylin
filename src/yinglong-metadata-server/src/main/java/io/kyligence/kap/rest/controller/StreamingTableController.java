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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.StreamingRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.service.StreamingTableService;
import io.kyligence.kap.rest.service.TableExtService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/streaming_tables", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class StreamingTableController extends NBasicController {

    @Autowired
    @Qualifier("streamingTableService")
    private StreamingTableService streamingTableService;

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @ApiOperation(value = "loadTables")
    @PostMapping(value = "/table", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> saveStreamingTable(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        String project = streamingRequest.getProject();
        checkProjectName(project);
        TableExtDesc tableExt = streamingTableService.getOrCreateTableExt(project, streamingRequest.getTableDesc());
        try {
            // If the Streaming Table does not have a BatchTable, convert decimal to double
            streamingTableService.decimalConvertToDouble(project, streamingRequest);

            streamingTableService.checkColumns(streamingRequest);

            tableExtService.checkAndLoadTable(project, streamingRequest.getTableDesc(), tableExt);
            streamingTableService.createKafkaConfig(project, streamingRequest.getKafkaConfig());
            LoadTableResponse loadTableResponse = new LoadTableResponse();
            loadTableResponse.getLoaded().add(streamingRequest.getTableDesc().getIdentity());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(RELOAD_TABLE_FAILED, root.getMessage());
        }
    }

    @ApiOperation(value = "updateTables")
    @PutMapping(value = "/table", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> updateStreamingTable(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        String project = streamingRequest.getProject();
        checkProjectName(project);
        try {
            TableExtDesc tableExt = streamingTableService.getOrCreateTableExt(project, streamingRequest.getTableDesc());
            streamingTableService.reloadTable(project, streamingRequest.getTableDesc(), tableExt);
            streamingTableService.updateKafkaConfig(project, streamingRequest.getKafkaConfig());
            LoadTableResponse loadTableResponse = new LoadTableResponse();
            loadTableResponse.getLoaded().add(streamingRequest.getTableDesc().getIdentity());
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(RELOAD_TABLE_FAILED, root.getMessage());
        }
    }
}
