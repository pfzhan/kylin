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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.rest.controller.BaseController;
import io.kyligence.kap.rest.controller.SampleController;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.response.OpenPartitionColumnFormatResponse;
import io.kyligence.kap.rest.service.TableService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenSampleController extends BaseController {

    private static final String TABLE = "table";

    @Autowired
    private SampleController sampleController;

    @Autowired
    private TableService tableService;

    /**
     * auto mode, refresh data
     *
     * @param request
     * @return
     * @throws IOException
     */
    @ApiOperation(value = "refreshSegments", tags = { "DW" })
    @PutMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        checkRequiredArg("tableName", request.getTable());

        getTable(request.getProject(), request.getTable());
        return sampleController.refreshSegments(request);
    }

    @ApiOperation(value = "samplingJobs", tags = { "AI" })
    @PostMapping(value = "/sampling_jobs")
    @ResponseBody
    public EnvelopeResponse<String> submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        checkStreamingOperation(request.getProject(), request.getQualifiedTableName());
        return sampleController.submitSampling(request);
    }

    @ApiOperation(value = "getPartitionColumnFormat", tags = { "DW" })
    @GetMapping(value = "/column_format")
    @ResponseBody
    public EnvelopeResponse<OpenPartitionColumnFormatResponse> getPartitionColumnFormat(
            @RequestParam(value = "project") String project, @RequestParam(value = "table") String table,
            @RequestParam(value = "column_name") String columnName) throws Exception {
        String projectName = checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("column_name", columnName);

        String columnFormat = tableService.getPartitionColumnFormat(projectName, table.toUpperCase(Locale.ROOT),
                columnName);
        OpenPartitionColumnFormatResponse columnFormatResponse = new OpenPartitionColumnFormatResponse();
        columnFormatResponse.setColumnName(columnName);
        columnFormatResponse.setColumnFormat(columnFormat);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, columnFormatResponse, "");
    }

    @VisibleForTesting
    protected TableDesc getTable(String project, String tableName) {
        TableDesc table = tableService.getTableManager(project).getTableDesc(tableName);
        if (null == table) {
            throw new KylinException(INVALID_TABLE_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableName));
        }
        return table;
    }

}
