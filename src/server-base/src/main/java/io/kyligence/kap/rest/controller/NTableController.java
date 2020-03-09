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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.PartitionKeyRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.ReloadTableRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.BatchLoadTableResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.PreReloadTableResponse;
import io.kyligence.kap.rest.response.PreUnloadTableResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableSamplingService;
import io.kyligence.kap.rest.service.TableService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_JSON })
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

    @ApiOperation(value = "getTableDesc (update)", notes = "Update Param: is_fuzzy, page_offset, page_size; Update Response: no format!")
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "true") boolean isFuzzy,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit)
            throws IOException {

        checkProjectName(project);
        List<TableDesc> tableDescs = new ArrayList<>();

        tableDescs.addAll(tableService.getTableDesc(project, withExt, table, database, isFuzzy));
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, getDataResponse("tables", tableDescs, offset, limit),
                "");
    }

    @ApiOperation(value = "getProjectTables (update)", notes = "Update Param: is_fuzzy, page_offset, page_size")
    @GetMapping(value = "/project_tables", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<NInitTablesResponse> getProjectTables(
            @RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "true") boolean isFuzzy,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) throws Exception {

        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                tableService.getProjectTables(project, table, offset, limit, false, (databaseName, tableName) -> {
                    return tableService.getTableDesc(project, withExt, tableName, databaseName, isFuzzy);
                }), "");
    }

    @ApiOperation(value = "unloadTable (update)", notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, // 
            @PathVariable(value = "table") String table,
            @RequestParam(value = "cascade", defaultValue = "false") Boolean cascade) {

        checkProjectName(project);
        String dbTblName = database + "." + table;
        tableService.unloadTable(project, dbTblName, cascade);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "prepareUnloadTable (update)", notes = "Update URL: {project}; Update Param: project")
    @GetMapping(value = "/{database:.+}/{table:.+}/prepare_unload",
            produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<PreUnloadTableResponse> prepareUnloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table) throws IOException {
        checkProjectName(project);
        val response = tableService.preUnloadTable(project, database + "." + table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    /**
     * set table partition key
     */
    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update Body: partition_column_format")
    @PostMapping(value = "/partition_key", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> setPartitionKey(@RequestBody PartitionKeyRequest partitionKeyRequest) {

        checkProjectName(partitionKeyRequest.getProject());
        tableService.setPartitionKey(partitionKeyRequest.getTable(), partitionKeyRequest.getProject(),
                partitionKeyRequest.getColumn(), partitionKeyRequest.getPartitionColumnFormat());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/top")
    @ResponseBody
    public EnvelopeResponse<String> setTableTop(@RequestBody TopTableRequest topTableRequest) {
        checkProjectName(topTableRequest.getProject());
        tableService.setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "loadTables (update)", notes = "Update Body: data_source_type, need_sampling, sampling_rows")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadTables(@RequestBody TableLoadRequest tableLoadRequest)
            throws Exception {
        checkProjectName(tableLoadRequest.getProject());
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(tableLoadRequest.getProject()) == null) {
            throw new BadRequestException(
                    String.format(MsgPicker.getMsg().getPROJECT_NOT_FOUND(), tableLoadRequest.getProject()));
        }
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

        if (!loadTableResponse.getLoaded().isEmpty() && Boolean.TRUE.equals(tableLoadRequest.getNeedSampling())) {
            checkSamplingRows(tableLoadRequest.getSamplingRows());
            tableSamplingService.sampling(loadTableResponse.getLoaded(), tableLoadRequest.getProject(),
                    tableLoadRequest.getSamplingRows());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, loadTableResponse, "");
    }

    @PostMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> setDateRanges(@RequestBody DateRangeRequest dateRangeRequest) throws Exception {
        checkProjectName(dateRangeRequest.getProject());
        checkRequiredArg(TABLE, dateRangeRequest.getTable());
        validateDataRange(dateRangeRequest.getStart(), dateRangeRequest.getEnd());
        tableService.setDataRange(dateRangeRequest.getProject(), dateRangeRequest);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "refreshSegments (update)", notes = "Update Body: refresh_start, refresh_end, affected_start, affected_end")
    @PutMapping(value = "/data_range")
    @ResponseBody
    public EnvelopeResponse<String> refreshSegments(@RequestBody RefreshSegmentsRequest request) throws IOException {
        checkProjectName(request.getProject());
        checkRequiredArg(TABLE, request.getTable());
        checkRequiredArg("refresh start", request.getRefreshStart());
        checkRequiredArg("refresh end", request.getRefreshEnd());
        checkRequiredArg("affected start", request.getAffectedStart());
        checkRequiredArg("affected end", request.getAffectedEnd());
        validateRange(request.getRefreshStart(), request.getRefreshEnd());
        modelService.refreshSegments(request.getProject(), request.getTable(), request.getRefreshStart(),
                request.getRefreshEnd(), request.getAffectedStart(), request.getAffectedEnd());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/data_range/latest_data", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<ExistedDataRangeResponse> getLatestData(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);

        ExistedDataRangeResponse response;
        try {
            response = tableService.getLatestDataRange(project, table);
        } catch (KylinTimeoutException ke) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    MsgPicker.getMsg().getPUSHDOWN_DATARANGE_TIMEOUT());
        } catch (Exception e) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    MsgPicker.getMsg().getPUSHDOWN_DATARANGE_ERROR());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }

    @GetMapping(value = "/partition_column_format")
    @ResponseBody
    public EnvelopeResponse<String> getPartitioinColumnFormat(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table,
            @RequestParam(value = "partition_column") String partitionColumn) throws Exception {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        checkRequiredArg("partitionColumn", partitionColumn);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                tableService.getPartitionColumnFormat(project, table, partitionColumn), "");
    }

    @GetMapping(value = "/batch_load")
    @ResponseBody
    public EnvelopeResponse<List<BatchLoadTableResponse>> getBatchLoadTables(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tableService.getBatchLoadTables(project), "");
    }

    @PostMapping(value = "/batch_load")
    @ResponseBody
    public EnvelopeResponse<String> batchLoad(@RequestBody List<DateRangeRequest> requests) throws Exception {
        if (requests.isEmpty())
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");

        checkArgsAndValidateRangeForBatchLoad(requests);
        tableService.batchLoadDataRange(requests.get(0).getProject(), requests);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/databases")
    @ResponseBody
    public EnvelopeResponse<List<String>> showDatabases(@RequestParam(value = "project") String project)
            throws Exception {

        checkProjectName(project);
        List<String> databases = tableService.getSourceDbNames(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, databases, "");
    }

    @GetMapping(value = "/loaded_databases", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Set<String>> getLoadedDatabases(@RequestParam(value = "project") String project) {
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
    @ApiOperation(value = "showTables (update)", notes = "Update Param: data_source_type, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/names")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TableNameResponse>>> showTables(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "data_source_type", required = false) Integer dataSourceType,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "database", required = true) String database) throws Exception {
        checkProjectName(project);
        List<TableNameResponse> tables = tableService.getTableNameResponses(project, database, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(tables, offset, limit), "");
    }

    @ApiOperation(value = "showProjectTableNames (update)", notes = "Update Param: data_source_type, page_offset, page_size")
    @GetMapping(value = "/project_table_names")
    @ResponseBody
    public EnvelopeResponse<NInitTablesResponse> showProjectTableNames(@RequestParam(value = "project") String project,
            @RequestParam(value = "data_source_type", required = false) Integer dataSourceType,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) throws Exception {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                tableService.getProjectTables(project, table, offset, limit, true, (databaseName, tableName) -> {
                    return tableService.getHiveTableNameResponses(project, databaseName, tableName);
                }), "");
    }

    @ApiOperation(value = "getTablesAndColomns (update)", notes = "Update Param: page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/simple_table")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TablesAndColumnsResponse>>> getTablesAndColomns(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<TablesAndColumnsResponse> responses = tableService.getTableAndColumns(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(responses, offset, limit), "");
    }

    @GetMapping(value = "/affected_data_range", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<RefreshAffectedSegmentsResponse> getRefreshAffectedDateRange(
            @RequestParam(value = "project") String project, @RequestParam(value = "table") String table,
            @RequestParam(value = "start") String start, @RequestParam(value = "end") String end) {
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

    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update Body: pushdown_range_limited")
    @PutMapping(value = "/pushdown_mode")
    @ResponseBody
    public EnvelopeResponse<String> setPushdownMode(@RequestBody PushDownModeRequest pushDownModeRequest) {
        checkProjectName(pushDownModeRequest.getProject());
        checkRequiredArg(TABLE, pushDownModeRequest.getTable());
        tableService.setPushDownMode(pushDownModeRequest.getProject(), pushDownModeRequest.getTable(),
                pushDownModeRequest.isPushdownRangeLimited());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/pushdown_mode")
    @ResponseBody
    public EnvelopeResponse<Boolean> getPushdownMode(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        boolean result = tableService.getPushDownMode(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
    }

    @GetMapping(value = "/auto_merge_config")
    @ResponseBody
    public EnvelopeResponse<AutoMergeConfigResponse> getAutoMergeConfig(
            @RequestParam(value = "model", required = false) String modelId,
            @RequestParam(value = "table", required = false) String tableName,
            @RequestParam(value = "project") String project) {
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

    @ApiOperation(value = "updateAutoMergeConfig (update)", notes = "Update Body: auto_merge_enabled, auto_merge_time_ranges, volatile_range_number, volatile_range_enabled, volatile_range_type")
    @PutMapping(value = "/auto_merge_config")
    @ResponseBody
    public EnvelopeResponse<String> updateAutoMergeConfig(@RequestBody AutoMergeRequest autoMergeRequest) {
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
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/sampling_jobs", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> submitSampling(@RequestBody SamplingRequest request) {
        checkProjectName(request.getProject());
        checkSamplingRows(request.getRows());
        checkSamplingTable(request.getQualifiedTableName());

        tableSamplingService.sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(),
                request.getRows());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "hasSamplingJob (update)", notes = "Update Param: qualified_table_name")
    @GetMapping(value = "/sampling_check_result")
    @ResponseBody
    public EnvelopeResponse<Boolean> hasSamplingJob(@RequestParam(value = "project") String project,
            @RequestParam(value = "qualified_table_name") String qualifiedTableName) {
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

    @GetMapping(value = "/prepare_reload")
    @ResponseBody
    public EnvelopeResponse<PreReloadTableResponse> preReloadTable(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        checkProjectName(project);
        try {
            val result = tableService.preProcessBeforeReload(project, table);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "");
        } catch (Exception e) {
            throw new BadRequestException("prepare reload table error", ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @PostMapping(value = "/reload", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> reloadTable(@RequestBody ReloadTableRequest request) {
        checkProjectName(request.getProject());
        if (StringUtils.isEmpty(request.getTable())) {
            throw new BadRequestException("table name must be specified!");
        }
        try {
            tableService.reloadTable(request.getProject(), request.getTable(), request.isNeedSample(),
                    request.getMaxRows(), request.isNeedBuild());
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
        } catch (Exception e) {
            throw new BadRequestException("reload table error", ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @ApiOperation(value = "reloadHiveTableName (update)", notes = "Update URL: table_name")
    @GetMapping(value = "/reload_hive_table_name")
    @ResponseBody
    public EnvelopeResponse<NHiveTableNameResponse> reloadHiveTablename(
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force) {
        try {
            NHiveTableNameResponse response = tableService.loadHiveTableNameToCache(force);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
        } catch (Exception e) {
            throw new BadRequestException("reload hive table name error", ResponseCode.CODE_UNDEFINED, e);
        }
    }

    @PostMapping(value = "/import_ssb")
    @ResponseBody
    public EnvelopeResponse<String> importSSBData() {
        tableService.importSSBDataBase();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/ssb")
    @ResponseBody
    public EnvelopeResponse<Boolean> checkSSB() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tableService.checkSSBDataBase(), "");
    }

}
