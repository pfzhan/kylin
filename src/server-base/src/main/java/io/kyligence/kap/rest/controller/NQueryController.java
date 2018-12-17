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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import io.kyligence.kap.rest.PagingUtil;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.rest.response.QueryEngineStatisticsResponse;
import io.kyligence.kap.rest.service.QueryHistoryService;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import io.kyligence.kap.rest.service.KapQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.collect.Maps;

/**
 * Handle query requests.
 *
 * @author xduo
 */
@RestController
@RequestMapping(value = "/query")
public class NQueryController extends NBasicController {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NQueryController.class);

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @Autowired
    @Qualifier("queryHistoryService")
    private QueryHistoryService queryHistoryService;

    @RequestMapping(value = "", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse query(@RequestBody PrepareSqlRequest sqlRequest) throws IOException {
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, sqlResponse, "");
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData

    @RequestMapping(value = "/prestate", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse prepareQuery(@RequestBody PrepareSqlRequest sqlRequest) throws IOException {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryService.doQueryWithCache(sqlRequest, false), "");
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public void saveQuery(@RequestBody SaveSqlRequest sqlRequest) throws IOException {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());
        queryService.saveQuery(creator, sqlRequest.getProject(), newQuery);
    }

    @RequestMapping(value = "/saved_queries/{project}/{id}", method = RequestMethod.DELETE, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void removeSavedQuery(@PathVariable("project") String project, @PathVariable("id") String id)
            throws IOException {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeSavedQuery(creator, project, id);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSavedQueries(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        List<Query> savedQueries = queryService.getSavedQueries(creator, project).getQueries();

        data.put("saved_queries", PagingUtil.cutPage(savedQueries, offset, limit));
        data.put("size", savedQueries.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/history_queries", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getQueryHistories(@RequestParam(value = "project") String project,
            @RequestParam(value = "startTimeFrom", required = false, defaultValue = "0") long startTimeFrom,
            @RequestParam(value = "startTimeTo", required = false, defaultValue = Long.MAX_VALUE + "") long startTimeTo,
            @RequestParam(value = "latencyFrom", required = false, defaultValue = "0") long latencyFrom,
            @RequestParam(value = "latencyTo", required = false, defaultValue = Integer.MAX_VALUE + "") long latencyTo,
            @RequestParam(value = "sql", required = false) String sql,
            @RequestParam(value = "realization", required = false) List<String> realizations,
            @RequestParam(value = "accelerateStatus", required = false) List<String> accelerateStatuses,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) {
        QueryHistoryRequest request = new QueryHistoryRequest(project, startTimeFrom, startTimeTo, latencyFrom,
                latencyTo, sql, realizations, accelerateStatuses);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryHistories(request, limit, offset), "");
    }

    @RequestMapping(value = "/format/{format}", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" }, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void downloadQueryResult(@PathVariable("format") String format, SQLRequest sqlRequest,
            HttpServletResponse response) throws IOException {

        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();

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

    @RequestMapping(value = "/tables_and_columns", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getMetadata(MetaRequest metaRequest) throws SQLException, IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryService.getMetadataV2(metaRequest.getProject()),
                "");
    }

    @RequestMapping(value = "/overview", method = RequestMethod.GET, params = { "start_time",
            "end_time" }, produces = { "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse<List<QueryEngineStatisticsResponse>> queryStatisticsByEngine(
            @RequestParam("project") String project, @RequestParam("start_time") long startTime,
            @RequestParam("end_time") long endTime) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                queryService.getQueryStatisticsByEngine(project, startTime, endTime), "");
    }

    @RequestMapping(value = "/statistics", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getQueryStatistics(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryStatistics(project, startTime, endTime), "");
    }

    @RequestMapping(value = "/statistics/count", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getQueryCount(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getQueryCount(project, startTime, endTime, dimension), "");
    }

    @RequestMapping(value = "/statistics/duration", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    public EnvelopeResponse getAvgDuration(@RequestParam("project") String project,
            @RequestParam("start_time") long startTime, @RequestParam("end_time") long endTime,
            @RequestParam("dimension") String dimension) {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                queryHistoryService.getAvgDuration(project, startTime, endTime, dimension), "");
    }
}
