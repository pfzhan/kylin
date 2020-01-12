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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.request.SQLFormatRequest;
import io.kyligence.kap.rest.response.QueryEngineStatisticsResponse;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import io.kyligence.kap.rest.service.KapQueryService;
import io.kyligence.kap.rest.service.QueryHistoryService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

/**
 * Handle query requests.
 *
 * @author xduo
 */
@RestController
@RequestMapping(value = "/api/query", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NQueryController extends NBasicController {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NQueryController.class);
    private static final Pattern queryNamePattern = Pattern.compile("^[a-zA-Z0-9_]*$");

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService queryHistoryService;

    @Autowired
    private ClusterManager clusterManager;

    @ApiOperation(value = "query (update)", notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key")
    @PostMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<SQLResponse> query(@RequestBody PrepareSqlRequest sqlRequest) {
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, sqlResponse, "");
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData

    @PostMapping(value = "/prestate")
    @ResponseBody
    public EnvelopeResponse<SQLResponse> prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, queryService.doQueryWithCache(sqlRequest, false), "");
    }

    @PostMapping(value = "/saved_queries")
    public EnvelopeResponse<String> saveQuery(@RequestBody SaveSqlRequest sqlRequest) throws IOException {
        String queryName = sqlRequest.getName();
        checkQueryName(queryName);
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(queryName, sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());
        queryService.saveQuery(creator, sqlRequest.getProject(), newQuery);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/saved_queries/{id:.+}")
    @ResponseBody
    public EnvelopeResponse<String> removeSavedQuery(@PathVariable("id") String id,
            @RequestParam("project") String project) throws IOException {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeSavedQuery(creator, project, id);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/saved_queries")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<Query>>> getSavedQueries(@RequestParam(value = "project") String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {
        checkProjectName(project);
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        List<Query> savedQueries = queryService.getSavedQueries(creator, project).getQueries();

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(savedQueries, offset, limit), "");
    }

    @ApiOperation(value = "getQueryHistories (update)", notes = "Update Param: start_time_from, start_time_to, latency_from, latency_to")
    @GetMapping(value = "/history_queries")
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getQueryHistories(@RequestParam(value = "project") String project,
            @RequestParam(value = "start_time_from", required = false) String startTimeFrom,
            @RequestParam(value = "start_time_to", required = false) String startTimeTo,
            @RequestParam(value = "latency_from", required = false) String latencyFrom,
            @RequestParam(value = "latency_to", required = false) String latencyTo,
            @RequestParam(value = "query_status", required = false) List<String> queryStatus,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "realization", required = false) List<String> realizations,
            @RequestParam(value = "server", required = false) String server,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo, latencyFrom,
                latencyTo, sql, server, queryStatus, realizations);
        checkGetQueryHistoriesParam(request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryHistories(request, limit, offset), "");
    }

    @GetMapping(value = "/servers")
    @ResponseBody
    public EnvelopeResponse<List<String>> getQueryServers() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, clusterManager.getQueryServers(), "");
    }

    private void checkGetQueryHistoriesParam(QueryHistoryRequest request) {
        // check start time and end time
        Preconditions.checkArgument(allEmptyOrNotAllEmpty(request.getStartTimeFrom(), request.getStartTimeTo()));
        Preconditions.checkArgument(allEmptyOrNotAllEmpty(request.getLatencyFrom(), request.getLatencyTo()));
    }

    private boolean allEmptyOrNotAllEmpty(String param1, String param2) {
        if (StringUtils.isEmpty(param1) && StringUtils.isEmpty(param2))
            return true;

        if (StringUtils.isNotEmpty(param1) && StringUtils.isNotEmpty(param2))
            return true;

        return false;
    }

    @PostMapping(value = "/format/{format:.+}", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void downloadQueryResult(@PathVariable("format") String format, SQLRequest sqlRequest,
            HttpServletResponse response) {

        KylinConfig config = queryService.getConfig();
        val msg = MsgPicker.getMsg();

        if ((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed())) {
            throw new ForbiddenException(msg.getEXPORT_RESULT_NOT_ALLOWED());
        }

        SQLResponse result = queryService.doQueryWithCache(sqlRequest, false);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String nowStr = sdf.format(new Date());
        response.setContentType("text/" + format + ";charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"" + nowStr + ".result." + format + "\"");
        ICsvListWriter csvWriter = null;

        try {
            //Add a BOM for Excel
            Writer writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
            writer.write('\uFEFF');

            csvWriter = new CsvListWriter(writer, CsvPreference.STANDARD_PREFERENCE);
            List<String> headerList = new ArrayList<String>();

            for (SelectedColumnMeta column : result.getColumnMetas()) {
                headerList.add(column.getLabel());
            }

            String[] headers = new String[headerList.size()];
            csvWriter.writeHeader(headerList.toArray(headers));

            for (List<String> row : result.getResults()) {
                csvWriter.write(row);
            }
        } catch (IOException e) {
            throw new InternalErrorException(e);
        } finally {
            IOUtils.closeQuietly(csvWriter);
        }
    }

    @ApiOperation(value = "getMetadata (update)", notes = "Update Param: project")
    @GetMapping(value = "/tables_and_columns")
    @ResponseBody
    public EnvelopeResponse<List<TableMetaWithType>> getMetadata(@RequestParam("project") String project)
            throws SQLException {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, queryService.getMetadataV2(project), "");
    }

    @GetMapping(value = "/overview", params = { "start_time", "end_time" })
    public EnvelopeResponse<QueryEngineStatisticsResponse> queryStatisticsByEngine(
            @RequestParam("project") String project, @RequestParam("start_time") long startTime,
            @RequestParam("end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                queryService.getQueryStatisticsByEngine(project, startTime, endTime), "");
    }

    @GetMapping(value = "/statistics")
    public EnvelopeResponse<QueryStatisticsResponse> getQueryStatistics(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryStatistics(project, startTime, endTime), "");
    }

    @GetMapping(value = "/statistics/count")
    public EnvelopeResponse<Map<String, Object>> getQueryCount(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryCount(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/statistics/duration")
    public EnvelopeResponse<Map<String, Object>> getAvgDuration(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        checkProjectName(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getAvgDuration(project, startTime, endTime, dimension), "");
    }

    @GetMapping(value = "/history_queries/table_names")
    public EnvelopeResponse<Map<String, String>> getQueryHistoryTableNames(
            @RequestParam(value = "projects", required = false) List<String> projects) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, queryHistoryService.getQueryHistoryTableMap(projects),
                "");
    }

    @PutMapping(value = "/format")
    public EnvelopeResponse<List<String>> formatQuery(@RequestBody SQLFormatRequest request) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, queryService.format(request.getSqls()), "");
    }

    private void checkQueryName(String queryName) {
        val msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(queryName)) {
            throw new BadRequestException(msg.getEMPTY_QUERY_NAME());
        }
        if (!queryNamePattern.matcher(queryName).matches()) {
            throw new BadRequestException(msg.getINVALID_QUERY_NAME());
        }
    }
}
