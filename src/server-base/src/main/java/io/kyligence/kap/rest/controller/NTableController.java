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
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Sets;

import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.PartitionKeyRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.ReloadTableRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableSamplingService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;

@Controller
@RequestMapping(value = "/tables")
@Component("TableController")
public class NTableController extends NBasicController {

    private static final String TABLE = "table";
    private static final int MAX_SAMPLING_ROWS = 20_000_000;
    private static final int MIN_SAMPLING_ROWS = 10_000;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @Autowired
    @Qualifier("tableSamplingService")
    private TableSamplingService tableSamplingService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "isFuzzy", required = false, defaultValue = "true") boolean isFuzzy,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) throws IOException {

        checkProjectName(project);
        List<TableDesc> tableDescs = new ArrayList<>();

        tableDescs.addAll(tableService.getTableDesc(project, withExt, table, database, isFuzzy));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, getDataResponse("tables", tableDescs, offset, limit),
                "");
    }

    @RequestMapping(value = "project_tables", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjectTables(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "isFuzzy", required = false, defaultValue = "true") boolean isFuzzy,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) throws Exception {

        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                tableService.getProjectTables(project, table, offset, limit, false, (databaseName, tableName) -> {
                    return tableService.getTableDesc(project, withExt, tableName, databaseName, isFuzzy);
                }), "");
    }

    @RequestMapping(value = "{project}/{database}/{table}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unloadTable(@PathVariable(value = "project") String project,
            @PathVariable(value = "database") String database, @PathVariable(value = "table") String table,
            @RequestParam(value = "cascade", defaultValue = "false") Boolean cascade) {

        checkProjectName(project);
        String dbTblName = database + "." + table;
        tableService.unloadTable(project, dbTblName, cascade);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @GetMapping(value = "{project}/{database}/{table}/prepare_unload", produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse prepareUnloadTable(@PathVariable(value = "project") String project,
            @PathVariable(value = "database") String database, @PathVariable(value = "table") String table)
            throws IOException {
        checkProjectName(project);
        val response = tableService.preUnloadTable(project, database + "." + table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    /**
     * set table partition key
     */
    @RequestMapping(value = "/partition_key", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setPartitionKey(@RequestBody PartitionKeyRequest partitionKeyRequest) {

        checkProjectName(partitionKeyRequest.getProject());
        tableService.setPartitionKey(partitionKeyRequest.getTable(), partitionKeyRequest.getProject(),
                partitionKeyRequest.getColumn());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/top", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setTableTop(@RequestBody TopTableRequest topTableRequest) {
        checkProjectName(topTableRequest.getProject());
        tableService.setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadTables(@RequestBody TableLoadRequest tableLoadRequest) throws Exception {
        Message msg = MsgPicker.getMsg();
        checkProjectName(tableLoadRequest.getProject());
        if (ArrayUtils.isEmpty(tableLoadRequest.getTables()) && ArrayUtils.isEmpty(tableLoadRequest.getDatabases())) {
            throw new BadRequestException("You should select at least one table or database to load!!");
        }
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        if (ArrayUtils.isNotEmpty(tableLoadRequest.getTables())) {
            Pair<String[], Set<String>> existsAndFails = tableService.classifyDbTables(tableLoadRequest.getProject(),
                    tableLoadRequest.getTables());
            LoadTableResponse loadByTable = tableExtService.loadTables(existsAndFails.getFirst(),
                    tableLoadRequest.getProject());
            loadTableResponse.getFailed().addAll(existsAndFails.getSecond());
            loadTableResponse.getFailed().addAll(loadByTable.getFailed());
            loadTableResponse.getLoaded().addAll(loadByTable.getLoaded());
        }

        if (ArrayUtils.isNotEmpty(tableLoadRequest.getDatabases())) {
            Pair<String[], Set<String>> existsAndFails = tableService.classifyDbTables(tableLoadRequest.getProject(),
                    tableLoadRequest.getDatabases());
            LoadTableResponse loadByDatabase = tableExtService.loadTablesByDatabase(tableLoadRequest.getProject(),
                    existsAndFails.getFirst());
            loadTableResponse.getFailed().addAll(existsAndFails.getSecond());
            loadTableResponse.getFailed().addAll(loadByDatabase.getFailed());
            loadTableResponse.getLoaded().addAll(loadByDatabase.getLoaded());
        }

        if (!loadTableResponse.getLoaded().isEmpty() && tableLoadRequest.isNeedSampling()) {
            checkSamplingRows(tableLoadRequest.getSamplingRows());
            tableSamplingService.sampling(loadTableResponse.getLoaded(), tableLoadRequest.getProject(),
                    tableLoadRequest.getSamplingRows());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, loadTableResponse, "");
    }

    @RequestMapping(value = "/data_range", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setDateRanges(@RequestBody DateRangeRequest dateRangeRequest) throws Exception {
        checkProjectName(dateRangeRequest.getProject());
        checkRequiredArg(TABLE, dateRangeRequest.getTable());
        validateDataRange(dateRangeRequest.getStart(), dateRangeRequest.getEnd());
        tableService.setDataRange(dateRangeRequest.getProject(), dateRangeRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/data_range", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
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

    @RequestMapping(value = "/data_range/latest_data", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getLatestData(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) throws Exception {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tableService.getLatestDataRange(project, table), "");
    }

    @RequestMapping(value = "/batch_load", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getBatchLoadTables(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tableService.getBatchLoadTables(project), "");
    }

    @RequestMapping(value = "/batch_load", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse batchLoad(@RequestBody List<DateRangeRequest> requests) throws Exception {
        if (requests.isEmpty())
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");

        checkArgsAndValidateRangeForBatchLoad(requests);
        tableService.batchLoadDataRange(requests.get(0).getProject(), requests);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @RequestMapping(value = "/databases", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showDatabases(@RequestParam(value = "project", required = true) String project)
            throws Exception {

        checkProjectName(project);
        List<String> databases = tableService.getSourceDbNames(project);
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
        List<TableNameResponse> tables = tableService.getTableNameResponses(project, database, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, getDataResponse("tables", tables, offset, limit), "");
    }

    @RequestMapping(value = "/project_table_names", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showProjectTableNames(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "datasourceType", required = false) Integer dataSourceType,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) throws Exception {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                tableService.getProjectTables(project, table, offset, limit, true, (databaseName, tableName) -> {
                    return tableService.getTableNameResponsesInCache(project, databaseName, tableName);
                }), "");
    }

    @RequestMapping(value = "/simple_table", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTablesAndColomns(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<TablesAndColumnsResponse> responses = tableService.getTableAndColumns(project);
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
        RefreshAffectedSegmentsResponse response = modelService.getRefreshAffectedSegmentsResponse(project, table,
                start, end);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
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
    public EnvelopeResponse getAutoMergeConfig(@RequestParam(value = "model", required = false) String modelId,
            @RequestParam(value = "table", required = false) String tableName,
            @RequestParam(value = "project", required = true) String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelId) && StringUtils.isEmpty(tableName)) {
            throw new BadRequestException("model name or table name must be specified!");
        }
        AutoMergeConfigResponse response;
        if (StringUtils.isNotEmpty(modelId)) {
            response = tableService.getAutoMergeConfigByModel(project, modelId);
        } else {
            response = tableService.getAutoMergeConfigByTable(project, tableName);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
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
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/sampling_jobs", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        checkSamplingRows(request.getRows());
        checkSamplingTable(request.getQualifiedTableName());

        tableSamplingService.sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(),
                request.getRows());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @GetMapping(value = "/sampling_check_result", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse hasSamplingJob(@RequestParam(value = "project") String project,
            @RequestParam(value = "qualifiedTableName") String qualifiedTableName) {
        checkProjectName(project);
        checkSamplingTable(qualifiedTableName);
        boolean hasSamplingJob = tableSamplingService.hasSamplingJob(project, qualifiedTableName);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, hasSamplingJob, "");
    }

    private void checkSamplingRows(int rows) {
        Message msg = MsgPicker.getMsg();
        if (rows > MAX_SAMPLING_ROWS) {
            throw new BadRequestException(String.format(msg.getBEYOND_MAX_SAMPLING_ROWS_HINT(), MAX_SAMPLING_ROWS));
        }

        if (rows < MIN_SAMPLING_ROWS) {
            throw new BadRequestException(String.format(msg.getBEYOND_MIX_SAMPLING_ROWSHINT(), MIN_SAMPLING_ROWS));
        }
    }

    private void checkSamplingTable(String tableName) {
        Message msg = MsgPicker.getMsg();
        if (tableName == null || StringUtils.isEmpty(tableName.trim())) {
            throw new BadRequestException(msg.getFAILED_FOR_NO_SAMPLING_TABLE());
        }

        if (tableName.contains(" ") || !tableName.contains(".") || tableName.split("\\.").length != 2) {
            throw new BadRequestException(String.format(msg.getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME(), tableName));
        }
    }

    @RequestMapping(value = "/prepare_reload", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse preReloadTable(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        checkProjectName(project);
        try {
            val result = tableService.preProcessBeforeReload(project, table);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
        } catch (Exception e) {
            throw new BadRequestException("prepare reload table error", ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @RequestMapping(value = "/reload", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse reloadTable(@RequestBody ReloadTableRequest request) {
        checkProjectName(request.getProject());
        if (StringUtils.isEmpty(request.getTable())) {
            throw new BadRequestException("table name must be specified!");
        }
        try {
            tableService.reloadTable(request.getProject(), request.getTable(), request.isNeedSample(),
                    request.getMaxRows());
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
        } catch (Exception e) {
            throw new BadRequestException("reload table error", ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @GetMapping(value = "/reload_hive_tablename", produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse reloadHiveTablename(
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force) {
        try {
            NHiveTableNameResponse response = tableService.loadHiveTableNameToCache(force);
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
        } catch (Exception e) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, e, "reload hive table name error");
        }

    }

}
