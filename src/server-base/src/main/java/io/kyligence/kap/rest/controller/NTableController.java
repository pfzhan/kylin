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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.FactTableRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;

@Controller
@RequestMapping(value = "/tables")
@Component("TableController")
public class NTableController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NTableController.class);

    private static final Message msg = MsgPicker.getMsg();

    private static final String TABLE = "table";

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "isFuzzy", required = false, defaultValue = "true") boolean isFuzzy,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {

        checkProjectName(project);
        List<TableDesc> tableDescs = new ArrayList<>();

        tableDescs.addAll(tableService.getTableDesc(project, withExt, table, database, isFuzzy));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, getDataResponse("tables", tableDescs, offset, limit),
                "");
    }

    @RequestMapping(value = "{project}/{database}/{table}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unloadTable(@PathVariable(value = "project") String project,
            @PathVariable(value = "database") String database, @PathVariable(value = "table") String table) {

        checkProjectName(project);
        String tableName = database + "." + table;
        if (modelService.isModelsUsingTable(tableName, project)) {
            List<String> models = modelService.getModelsUsingTable(tableName, project);
            throw new BadRequestException(String.format(msg.getTABLE_IN_USE_BY_MODEL(), models));
        }
        tableService.unloadTable(project, tableName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    /**
     * set table fact
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/fact", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setTableFact(@RequestBody FactTableRequest factTableRequest) {

        checkProjectName(factTableRequest.getProject());
        if (factTableRequest.isFact()) {
            checkRequiredArg("column", factTableRequest.getColumn());
        }
        tableService.setFact(factTableRequest.getTable(), factTableRequest.getProject(), factTableRequest.isFact(),
                factTableRequest.getColumn(), factTableRequest.getPartitionDateFormat());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/top", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setTableTop(@RequestBody TopTableRequest topTableRequest) {
        checkProjectName(topTableRequest.getProject());
        tableService.setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadTables(@RequestBody TableLoadRequest tableLoadRequest) throws Exception {

        checkProjectName(tableLoadRequest.getProject());
        if (ArrayUtils.isEmpty(tableLoadRequest.getTables()) && ArrayUtils.isEmpty(tableLoadRequest.getDatabases())) {
            throw new BadRequestException("You should select at least one table or database to load!!");
        }
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        if (ArrayUtils.isNotEmpty(tableLoadRequest.getTables())) {
            LoadTableResponse loadByTable = tableExtService.loadTables(tableLoadRequest.getTables(),
                    tableLoadRequest.getProject(), tableLoadRequest.getDatasourceType());
            loadTableResponse.getFailed().addAll(loadByTable.getFailed());
            loadTableResponse.getLoaded().addAll(loadByTable.getLoaded());
        }
        if (ArrayUtils.isNotEmpty(tableLoadRequest.getDatabases())) {

            LoadTableResponse loadByDatabase = tableExtService.loadTablesByDatabase(tableLoadRequest.getProject(),
                    tableLoadRequest.getDatabases(), tableLoadRequest.getDatasourceType());
            loadTableResponse.getFailed().addAll(loadByDatabase.getFailed());
            loadTableResponse.getLoaded().addAll(loadByDatabase.getLoaded());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, loadTableResponse, "");
    }

    @RequestMapping(value = "/data_range", method = {RequestMethod.POST}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse setDateRanges(@RequestBody DateRangeRequest dateRangeRequest) throws IOException {
        checkProjectName(dateRangeRequest.getProject());
        checkRequiredArg(TABLE, dateRangeRequest.getTable());
        validateRange(dateRangeRequest.getStart(), dateRangeRequest.getEnd());
        tableService.setDataRange(dateRangeRequest.getProject(), dateRangeRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/databases", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showDatabases(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "datasourceType", required = false) Integer datasourceType) throws Exception {

        checkProjectName(project);
        List<String> databases = tableService.getSourceDbNames(project, datasourceType);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, databases, "");
    }

    @RequestMapping(value = "/loaded_databases", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getLoadedDatabases(@RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        Set<String> loadedDatabases = tableService.getLoadedDatabases(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, loadedDatabases, "");
    }

    /**
     * Show all tablesNames
     *
     * @return String[]
     * @throws IOException
     */
    @RequestMapping(value = "/names", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showTables(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "datasourceType", required = false) Integer dataSourceType,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "database", required = true) String database) throws Exception {
        checkProjectName(project);
        List<TableNameResponse> tables = tableService.getTableNameResponses(project, database, dataSourceType, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, getDataResponse("tables", tables, offset, limit), "");
    }

    @RequestMapping(value = "/simple_table", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTablesAndColomns(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<TablesAndColumnsResponse> responses = tableService.getTableAndColomns(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                getDataResponse("tablesAndColumns", responses, offset, limit), "");
    }

    @RequestMapping(value = "/affected_data_range", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getRefreshAffectedDateRange(
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = true) String table,
            @RequestParam(value = "start", required = true) String start,
            @RequestParam(value = "end", required = true) String end) {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("start", start);
        checkRequiredArg("end", end);
        validateRange(start, end);
        tableService.checkRefreshDataRangeReadiness(project, table, start, end);
        RefreshAffectedSegmentsResponse response = modelService.getAffectedSegmentsResponse(project, table, start, end,
                ManagementType.TABLE_ORIENTED);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/data_range", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse refreshSegments(@RequestBody RefreshSegmentsRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(TABLE, request.getTable());
        checkRequiredArg("refresh start", request.getRefreshStart());
        checkRequiredArg("refresh end", request.getRefreshEnd());
        checkRequiredArg("affected start", request.getAffectedStart());
        checkRequiredArg("affected end", request.getAffectedEnd());
        validateRange(request.getRefreshStart(), request.getRefreshEnd());
        modelService.refreshSegments(request.getProject(), request.getTable(), request.getRefreshStart(),
                request.getRefreshEnd(), request.getAffectedStart(), request.getAffectedEnd());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/pushdown_mode", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setPushdownMode(@RequestBody PushDownModeRequest pushDownModeRequest) {
        checkProjectName(pushDownModeRequest.getProject());
        checkRequiredArg(TABLE, pushDownModeRequest.getTable());
        tableService.setPushDownMode(pushDownModeRequest.getProject(), pushDownModeRequest.getTable(),
                pushDownModeRequest.isPushdownRangeLimited());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/pushdown_mode", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getPushdownMode(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = true) String table) {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        boolean result = tableService.getPushDownMode(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @RequestMapping(value = "/auto_merge_config", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAutoMergeConfig(@RequestParam(value = "model", required = false) String modelName,
            @RequestParam(value = "table", required = false) String tableName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelName) && StringUtils.isEmpty(tableName)) {
            throw new BadRequestException("model name or table name must be specified!");
        }
        AutoMergeConfigResponse response;
        if (StringUtils.isNotEmpty(modelName)) {
            response = tableService.getAutoMergeConfigByModel(project, modelName);
        } else {
            response = tableService.getAutoMergeConfigByTable(project, tableName);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    @RequestMapping(value = "/auto_merge_config", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateAutoMergeConfig(@RequestBody AutoMergeRequest autoMergeRequest) {
        checkProjectName(autoMergeRequest.getProject());
        if (ArrayUtils.isEmpty(autoMergeRequest.getAutoMergeTimeRanges())) {
            throw new BadRequestException("You should specify at least one autoMerge range!");
        }
        if (StringUtils.isEmpty(autoMergeRequest.getModel()) && StringUtils.isEmpty(autoMergeRequest.getTable())) {
            throw new BadRequestException("model name or table name must be specified!");
        }
        if (StringUtils.isNotEmpty(autoMergeRequest.getModel())) {
            tableService.setAutoMergeConfigByModel(autoMergeRequest.getProject(), autoMergeRequest);
        } else {
            tableService.setAutoMergeConfigByTable(autoMergeRequest.getProject(), autoMergeRequest);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

}
