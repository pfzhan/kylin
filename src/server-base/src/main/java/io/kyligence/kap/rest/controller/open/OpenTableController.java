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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_SAMPLING_RANGE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_DATA_SOURCE_TYPE;

import java.io.IOException;
import java.util.List;

import io.kyligence.kap.rest.response.OpenPreReloadTableResponse;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
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

import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NTableController;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.TableService;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenTableController extends NBasicController {

    @Autowired
    private NTableController tableController;

    @Autowired
    private TableService tableService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private AclEvaluate aclEvaluate;

    private static final Integer MAX_SAMPLING_ROWS = 20_000_000;
    private static final Integer MIN_SAMPLING_ROWS = 10_000;

    @VisibleForTesting
    public TableDesc getTable(String project, String tableName) {
        TableDesc table = tableService.getTableManager(project).getTableDesc(tableName);
        if (null == table) {
            throw new KylinException(INVALID_TABLE_NAME, String.format(MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableName));
        }
        return table;
    }

    @VisibleForTesting
    public void updateDataSourceType(String project, int dataSourceType) {
        projectService.setDataSourceType(project, String.valueOf(dataSourceType));
    }

    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TableDesc>>> getTableDesc(@RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "ext", required = false, defaultValue = "true") boolean withExt,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit)
            throws IOException {
        List<TableDesc> result = tableService.getTableDesc(project, withExt, table, database, isFuzzy);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(result, offset, limit), "");
    }

    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadTables(@RequestBody TableLoadRequest tableLoadRequest)
            throws Exception {
        checkProjectName(tableLoadRequest.getProject());
        checkRequiredArg("need_sampling", tableLoadRequest.getNeedSampling());
        if (Boolean.TRUE.equals(tableLoadRequest.getNeedSampling())
                && (null == tableLoadRequest.getSamplingRows() || tableLoadRequest.getSamplingRows() > MAX_SAMPLING_ROWS
                        || tableLoadRequest.getSamplingRows() < MIN_SAMPLING_ROWS)) {
            throw new KylinException(INVALID_SAMPLING_RANGE,
                    "Invalid parameters, please check whether the number of sampling rows is between 10000 and 20000000.");
        }

        // default set data_source_type = 9
        if (ISourceAware.ID_SPARK != tableLoadRequest.getDataSourceType()) {
            throw new KylinException(UNSUPPORTED_DATA_SOURCE_TYPE, "Only support Hive as the data source. (data_source_type = 9)");
        }
        updateDataSourceType(tableLoadRequest.getProject(), tableLoadRequest.getDataSourceType());

        return tableController.loadTables(tableLoadRequest);
    }

    /**
     * auto mode, load data
     * @param request
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> setDateRanges(@RequestBody DateRangeRequest request) throws Exception {
        checkProjectName(request.getProject());
        checkRequiredArg("tableName", request.getTable());

        getTable(request.getProject(), request.getTable());
        return tableController.setDateRanges(request);
    }

    /**
     * auto mode, refresh data
     * @param request
     * @return
     * @throws IOException
     */
    @PutMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
        checkProjectName(request.getProject());
        checkRequiredArg("tableName", request.getTable());

        getTable(request.getProject(), request.getTable());
        return tableController.refreshSegments(request);
    }

    @GetMapping(value = "/pre_reload")
    @ResponseBody
    public EnvelopeResponse<OpenPreReloadTableResponse> preReloadTable(@RequestParam(value = "project") String project,
                                                                       @RequestParam(value = "table") String table) {
        try {
            checkProjectName(project);
            OpenPreReloadTableResponse result = tableService.preProcessBeforeReloadWithoutFailFast(project, table);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
        } catch (Exception e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            throw new KylinException(RELOAD_TABLE_FAILED, root.getMessage());
        }
    }
}
