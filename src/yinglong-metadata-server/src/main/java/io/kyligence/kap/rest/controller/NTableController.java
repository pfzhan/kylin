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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_REFRESH_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import io.kyligence.kap.rest.delegate.TableSamplingInvoker;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.TableRefresh;
import org.apache.kylin.rest.response.TableRefreshAll;
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

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AWSTableLoadRequest;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.MergeAndUpdateTableExtRequest;
import io.kyligence.kap.rest.request.PartitionKeyRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.ReloadTableRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.request.UpdateAWSTableExtDescRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.PreReloadTableResponse;
import io.kyligence.kap.rest.response.PreUnloadTableResponse;
import io.kyligence.kap.rest.response.RefreshAffectedSegmentsResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.response.UpdateAWSTableExtDescResponse;
import io.kyligence.kap.rest.service.ModelBuildSupporter;
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
    @Qualifier("modelBuildService")
    private ModelBuildSupporter modelBuildService;

    @Autowired
    private TableSamplingInvoker tableSamplingInvoker;

    @ApiOperation(value = "getTableDesc", tags = {
            "AI" }, notes = "Update Param: is_fuzzy, page_offset, page_size; Update Response: no format!")
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "source_type", required = false, defaultValue = "9") Integer sourceType)
            throws IOException {

        checkProjectName(project);
        List<TableDesc> tableDescs = new ArrayList<>();

        tableDescs.addAll(tableService.getTableDescByType(project, withExt, table, database, isFuzzy, sourceType));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, getDataResponse("tables", tableDescs, offset, limit),
                "");
    }

    @ApiOperation(value = "getProjectTables", tags = { "AI" }, notes = "Update Param: is_fuzzy, page_offset, page_size")
    @GetMapping(value = "/project_tables", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<NInitTablesResponse> getProjectTables(
            @RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "source_type", required = false, defaultValue = "9") List<Integer> sourceType)
            throws Exception {

        String projectName = checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                tableService.getProjectTables(projectName, table, offset, limit, false, (databaseName, tableName) -> {
                    return tableService.getTableDescByTypes(projectName, withExt, tableName, databaseName, isFuzzy,
                            sourceType);
                }), "");
    }

    @ApiOperation(value = "unloadTable", tags = { "AI" }, notes = "Update URL: {project}; Update Param: project")
    @DeleteMapping(value = "/{database:.+}/{table:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table,
            @RequestParam(value = "cascade", defaultValue = "false") Boolean cascade) {

        checkProjectName(project);
        String dbTblName = database + "." + table;
        tableService.unloadTable(project, dbTblName, cascade);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "prepareUnloadTable", tags = { "AI" }, notes = "Update URL: {project}; Update Param: project")
    @GetMapping(value = "/{database:.+}/{table:.+}/prepare_unload", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<PreUnloadTableResponse> prepareUnloadTable(@RequestParam(value = "project") String project,
            @PathVariable(value = "database") String database, //
            @PathVariable(value = "table") String table) throws IOException {
        checkProjectName(project);
        val response = tableService.preUnloadTable(project, database + "." + table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    /**
     * set table partition key
     */
    @ApiOperation(value = "partitionKey", tags = { "AI" }, notes = "Update Body: partition_column_format")
    @PostMapping(value = "/partition_key", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> setPartitionKey(@RequestBody PartitionKeyRequest partitionKeyRequest) {

        checkProjectName(partitionKeyRequest.getProject());
        if (partitionKeyRequest.getPartitionColumnFormat() != null) {
            validateDateTimeFormatPattern(partitionKeyRequest.getPartitionColumnFormat());
        }
        tableService.setPartitionKey(partitionKeyRequest.getTable(), partitionKeyRequest.getProject(),
                partitionKeyRequest.getColumn(), partitionKeyRequest.getPartitionColumnFormat());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "makeTop", tags = { "AI" })
    @PostMapping(value = "/top")
    @ResponseBody
    public EnvelopeResponse<String> setTableTop(@RequestBody TopTableRequest topTableRequest) {
        checkProjectName(topTableRequest.getProject());
        tableService.setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "loadTables", tags = {
            "AI" }, notes = "Update Body: data_source_type, need_sampling, sampling_rows")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadTables(@RequestBody TableLoadRequest tableLoadRequest)
            throws Exception {
        checkProjectName(tableLoadRequest.getProject());
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(tableLoadRequest.getProject()) == null) {
            throw new KylinException(PROJECT_NOT_EXIST, tableLoadRequest.getProject());
        }
        if (ArrayUtils.isEmpty(tableLoadRequest.getTables()) && ArrayUtils.isEmpty(tableLoadRequest.getDatabases())) {
            throw new KylinException(EMPTY_PARAMETER, "You should select at least one table or database to load!!");
        }

        LoadTableResponse loadTableResponse = new LoadTableResponse();
        if (ArrayUtils.isNotEmpty(tableLoadRequest.getTables())) {
            LoadTableResponse loadByTable = tableExtService.loadDbTables(tableLoadRequest.getTables(),
                    tableLoadRequest.getProject(), false);
            loadTableResponse.getFailed().addAll(loadByTable.getFailed());
            loadTableResponse.getLoaded().addAll(loadByTable.getLoaded());
        }

        if (ArrayUtils.isNotEmpty(tableLoadRequest.getDatabases())) {
            LoadTableResponse loadByDb = tableExtService.loadDbTables(tableLoadRequest.getDatabases(),
                    tableLoadRequest.getProject(), true);
            loadTableResponse.getFailed().addAll(loadByDb.getFailed());
            loadTableResponse.getLoaded().addAll(loadByDb.getLoaded());
        }

        if (!loadTableResponse.getLoaded().isEmpty() && Boolean.TRUE.equals(tableLoadRequest.getNeedSampling())) {
            checkSamplingRows(tableLoadRequest.getSamplingRows());
            tableSamplingInvoker.sampling(loadTableResponse.getLoaded(), tableLoadRequest.getProject(),
                    tableLoadRequest.getSamplingRows(), tableLoadRequest.getPriority(), tableLoadRequest.getYarnQueue(),
                    tableLoadRequest.getTag());
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
    }

    @ApiOperation(value = "loadAWSTablesCompatibleCrossAccount", tags = {"KC" },
            notes = "Update Body: data_source_type, need_sampling, sampling_rows, data_source_properties")
    @PostMapping(value = "/compatibility/aws")
    @ResponseBody
    public EnvelopeResponse<LoadTableResponse> loadAWSTablesCompatibleCrossAccount(@RequestBody AWSTableLoadRequest tableLoadRequest)
            throws Exception {
        checkProjectName(tableLoadRequest.getProject());
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(tableLoadRequest.getProject()) == null) {
            throw new KylinException(PROJECT_NOT_EXIST, tableLoadRequest.getProject());
        }
        if (CollectionUtils.isEmpty(tableLoadRequest.getTables())) {
            throw new KylinException(EMPTY_PARAMETER, "tables parameter must be not null !");
        }

        LoadTableResponse loadTableResponse = new LoadTableResponse();
        LoadTableResponse loadByTable = tableExtService.loadAWSTablesCompatibleCrossAccount(tableLoadRequest.getTables(),
                tableLoadRequest.getProject());
        loadTableResponse.getFailed().addAll(loadByTable.getFailed());
        loadTableResponse.getLoaded().addAll(loadByTable.getLoaded());

        if (!loadTableResponse.getLoaded().isEmpty() && Boolean.TRUE.equals(tableLoadRequest.getNeedSampling())) {
            TableSamplingService.checkSamplingRows(tableLoadRequest.getSamplingRows());
            tableSamplingService.sampling(loadTableResponse.getLoaded(), tableLoadRequest.getProject(),
                    tableLoadRequest.getSamplingRows(), tableLoadRequest.getPriority(), tableLoadRequest.getYarnQueue(),
                    tableLoadRequest.getTag());
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadTableResponse, "");
    }

    @ApiOperation(value = "updateLoadedAWSTableExtProp", tags = {"KC" }, notes = "Update Body: data_source_properties")
    @PutMapping(value = "/ext/prop/aws")
    @ResponseBody
    public EnvelopeResponse<UpdateAWSTableExtDescResponse> updateLoadedAWSTableExtProp(@RequestBody UpdateAWSTableExtDescRequest request) {
        checkProjectName(request.getProject());
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(request.getProject()) == null) {
            throw new KylinException(PROJECT_NOT_EXIST, request.getProject());
        }
        if (CollectionUtils.isEmpty(request.getTables())) {
            throw new KylinException(EMPTY_PARAMETER, "tables parameter must be not null !");
        }

        UpdateAWSTableExtDescResponse updateTableExtDescResponse = tableExtService.updateAWSLoadedTableExtProp(request);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, updateTableExtDescResponse, "");
    }

    @ApiOperation(value = "databases", tags = { "AI" })
    @GetMapping(value = "/databases")
    @ResponseBody
    public EnvelopeResponse<List<String>> showDatabases(@RequestParam(value = "project") String project)
            throws Exception {
        checkProjectName(project);
        List<String> databases = tableService.getSourceDbNames(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, databases, "");
    }

    @ApiOperation(value = "loadedDatabases", tags = { "AI" })
    @GetMapping(value = "/loaded_databases", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Set<String>> getLoadedDatabases(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        Set<String> loadedDatabases = tableService.getLoadedDatabases(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, loadedDatabases, "");
    }

    /**
     * Show all tablesNames
     *
     * @return String[]
     * @throws IOException
     */
    @ApiOperation(value = "showTables", tags = {
            "AI" }, notes = "Update Param: data_source_type, page_offset, page_size; Update Response: total_size")
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(tables, offset, limit), "");
    }

    @ApiOperation(value = "showProjectTableNames", tags = {
            "AI" }, notes = "Update Param: data_source_type, page_offset, page_size")
    @GetMapping(value = "/project_table_names")
    @ResponseBody
    public EnvelopeResponse<NInitTablesResponse> showProjectTableNames(@RequestParam(value = "project") String project,
            @RequestParam(value = "data_source_type", required = false) Integer dataSourceType,
            @RequestParam(value = "table", required = false, defaultValue = "") String table,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) throws Exception {
        String projectName = checkProjectName(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                tableService.getProjectTables(projectName, table, offset, limit, true, (databaseName,
                        tableName) -> tableService.getHiveTableNameResponses(projectName, databaseName, tableName)),
                "");
    }

    @ApiOperation(value = "getTablesAndColumns", tags = {
            "AI" }, notes = "Update Param: page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/simple_table")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<TablesAndColumnsResponse>>> getTablesAndColomns(
            @RequestParam(value = "project") String project,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        List<TablesAndColumnsResponse> responses = tableService.getTableAndColumns(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(responses, offset, limit), "");
    }

    @ApiOperation(value = "affectedDataRange", tags = { "AI" })
    @GetMapping(value = "/affected_data_range", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
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
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updatePushdownMode", tags = { "AI" }, notes = "Update Body: pushdown_range_limited")
    @PutMapping(value = "/pushdown_mode")
    @ResponseBody
    public EnvelopeResponse<String> setPushdownMode(@RequestBody PushDownModeRequest pushDownModeRequest) {
        checkProjectName(pushDownModeRequest.getProject());
        checkRequiredArg(TABLE, pushDownModeRequest.getTable());
        tableService.setPushDownMode(pushDownModeRequest.getProject(), pushDownModeRequest.getTable(),
                pushDownModeRequest.isPushdownRangeLimited());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getPushdownMode", tags = { "AI" })
    @GetMapping(value = "/pushdown_mode")
    @ResponseBody
    public EnvelopeResponse<Boolean> getPushdownMode(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        checkProjectName(project);
        checkRequiredArg(TABLE, table);
        boolean result = tableService.getPushDownMode(project, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "autoMergeConfig", tags = { "DW" })
    @GetMapping(value = "/auto_merge_config")
    @ResponseBody
    public EnvelopeResponse<AutoMergeConfigResponse> getAutoMergeConfig(
            @RequestParam(value = "model", required = false) String modelId,
            @RequestParam(value = "table", required = false) String tableName,
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        if (StringUtils.isEmpty(modelId) && StringUtils.isEmpty(tableName)) {
            throw new KylinException(EMPTY_PARAMETER, "model name or table name must be specified!");
        }
        AutoMergeConfigResponse response;
        if (StringUtils.isNotEmpty(modelId)) {
            response = tableService.getAutoMergeConfigByModel(project, modelId);
        } else {
            response = tableService.getAutoMergeConfigByTable(project, tableName);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updateAutoMergeConfig", tags = {
            "DW" }, notes = "Update Body: auto_merge_enabled, auto_merge_time_ranges, volatile_range_number, volatile_range_enabled, volatile_range_type")
    @PutMapping(value = "/auto_merge_config")
    @ResponseBody
    public EnvelopeResponse<String> updateAutoMergeConfig(@RequestBody AutoMergeRequest autoMergeRequest) {
        checkProjectName(autoMergeRequest.getProject());
        if (ArrayUtils.isEmpty(autoMergeRequest.getAutoMergeTimeRanges())) {
            throw new KylinException(EMPTY_PARAMETER, "You should specify at least one autoMerge range!");
        }
        if (StringUtils.isEmpty(autoMergeRequest.getModel()) && StringUtils.isEmpty(autoMergeRequest.getTable())) {
            throw new KylinException(EMPTY_PARAMETER, "model name or table name must be specified!");
        }
        if (StringUtils.isNotEmpty(autoMergeRequest.getModel())) {
            tableService.setAutoMergeConfigByModel(autoMergeRequest.getProject(), autoMergeRequest);
        } else {
            tableService.setAutoMergeConfigByTable(autoMergeRequest.getProject(), autoMergeRequest);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "prepareReload", tags = { "AI" })
    @GetMapping(value = "/prepare_reload", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<PreReloadTableResponse> preReloadTable(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) throws Exception {

        checkProjectName(project);
        val result = tableService.preProcessBeforeReloadWithFailFast(project, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "reload", tags = { "AI" })
    @PostMapping(value = "/reload", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> reloadTable(@RequestBody ReloadTableRequest request) {
        checkProjectName(request.getProject());
        if (StringUtils.isEmpty(request.getTable())) {
            throw new KylinException(INVALID_TABLE_NAME, MsgPicker.getMsg().getTableNameCannotEmpty());
        }
        if (request.isNeedSample()) {
            TableSamplingService.checkSamplingRows(request.getMaxRows());
        }
        tableService.reloadTable(request.getProject(), request.getTable(), request.isNeedSample(), request.getMaxRows(),
                request.isNeedBuild(), request.getPriority(), request.getYarnQueue());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "reloadHiveTableName", tags = { "AI" }, notes = "Update URL: table_name")
    @GetMapping(value = "/reload_hive_table_name")
    @ResponseBody
    public EnvelopeResponse<NHiveTableNameResponse> reloadHiveTablename(
            @RequestParam(value = "project", required = true, defaultValue = "") String project,
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force) throws Exception {
        NHiveTableNameResponse response = tableService.loadProjectHiveTableNameToCacheImmediately(project, force);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "importSSB", tags = { "DW" })
    @PostMapping(value = "/import_ssb")
    @ResponseBody
    public EnvelopeResponse<String> importSSBData() {
        tableService.importSSBDataBase();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "importSSB", tags = { "DW" })
    @GetMapping(value = "/ssb")
    @ResponseBody
    public EnvelopeResponse<Boolean> checkSSB() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, tableService.checkSSBDataBase(), "");
    }

    @ApiOperation(value = "catalogCache", tags = { "DW" })
    @PutMapping(value = "single_catalog_cache", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<TableRefresh> refreshSingleCatalogCache(@RequestBody HashMap refreshRequest) {
        checkRefreshParam(refreshRequest);
        TableRefresh response = tableService.refreshSingleCatalogCache(refreshRequest);
        return new EnvelopeResponse<>(response.getCode(), response, response.getMsg());
    }

    @ApiOperation(value = "catalogCache", tags = { "DW" })
    @PutMapping(value = "catalog_cache", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse refreshCatalogCache(final HttpServletRequest refreshRequest) {
        TableRefreshAll response = tableService.refreshAllCatalogCache(refreshRequest);
        return new EnvelopeResponse<>(response.getCode(), response, response.getMsg());
    }

    @ApiOperation(value = "modelTables", tags = { "AI" })
    @GetMapping(value = "/model_tables")
    @ResponseBody
    public EnvelopeResponse getModelTables(@RequestParam("project") String project,
            @RequestParam("model_name") String modelName) {
        checkProjectName(project);
        val res = tableService.getTablesOfModel(project, modelName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, res, "");
    }

    private void checkRefreshParam(Map refreshRequest) {
        val message = MsgPicker.getMsg();
        Object tables = refreshRequest.get("tables");
        if (tables == null) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamInvalid(), false);
        } else if (refreshRequest.keySet().size() > 1) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamMore(), false);
        } else if (!(tables instanceof List)) {
            throw new KylinException(INVALID_TABLE_REFRESH_PARAMETER, message.getTableRefreshParamInvalid(), false);
        }
    }

    @ApiOperation(value = "get tableDesc", tags = {
            "AI" }, notes = "helper method for RPC-invoke get tableDesc")
    @GetMapping(value = "/feign/get_table_desc")
    @ResponseBody
    public TableDesc getTableDesc(@RequestParam(value = "project") String project,
            @RequestParam(value = "table") String table) {
        return tableExtService.getTableDesc(project, table);
    }

    @GetMapping(value = "/feign/get_table_names_by_fuzzy_key")
    @ResponseBody
    public List<String> getTableNamesByFuzzyKey(@RequestParam(value = "project") String project,
                                                @RequestParam(value = "fuzzyKey") String fuzzyKey) {
        return tableExtService.getTableNamesByFuzzyKey(project, fuzzyKey);
    }

    @PostMapping(value = "/feign/merge_and_update_table_ext")
    @ResponseBody
    public void mergeAndUpdateTableExt(@RequestParam("project") String project,
                                       @RequestBody MergeAndUpdateTableExtRequest request) {
        tableExtService.mergeAndUpdateTableExt(project, request);
    }

    @PostMapping(value = "/feign/save_table_ext")
    @ResponseBody
    public void saveTableExt(@RequestParam("project") String project, @RequestBody TableExtDesc tableExt) {
        tableExtService.saveTableExt(project, tableExt);
    }

    @PostMapping(value = "/feign/update_table_desc")
    @ResponseBody
    public void updateTableDesc(@RequestParam("project") String project, @RequestBody TableDesc tableDesc) {
        tableExtService.updateTableDesc(project, tableDesc);
    }
}
