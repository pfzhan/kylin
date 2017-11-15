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

package io.kyligence.kap.rest.controller2;

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

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
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

import io.kyligence.kap.rest.service.KapQueryService;

/**
 * Handle query requests.
 * 
 * @author xduo
 */
@Controller
public class QueryControllerV2 extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(QueryControllerV2.class);

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse queryV2(@RequestBody PrepareSqlRequest sqlRequest) {
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, sqlResponse, "");
    }

    // TODO should be just "prepare" a statement, get back expected ResultSetMetaData

    @RequestMapping(value = "/query/prestate", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse prepareQueryV2(@RequestBody PrepareSqlRequest sqlRequest) {
        Map<String, String> newToggles = Maps.newHashMap();
        if (sqlRequest.getBackdoorToggles() != null)
            newToggles.putAll(sqlRequest.getBackdoorToggles());
        newToggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
        sqlRequest.setBackdoorToggles(newToggles);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryService.doQueryWithCache(sqlRequest), "");
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void saveQueryV2(@RequestBody SaveSqlRequest sqlRequest) throws IOException {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        Query newQuery = new Query(sqlRequest.getName(), sqlRequest.getProject(), sqlRequest.getSql(),
                sqlRequest.getDescription());

        queryService.saveQuery(creator, newQuery);
    }

    @RequestMapping(value = "/saved_queries/{id}", method = RequestMethod.DELETE, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void removeQueryV2(@PathVariable String id) throws IOException {

        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        queryService.removeQuery(creator, id);
    }

    @RequestMapping(value = "/saved_queries", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getQueriesV2(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        String creator = SecurityContextHolder.getContext().getAuthentication().getName();
        List<Query> queries = new ArrayList<Query>();
        for (Query query : queryService.getQueries(creator)) {
            if (project == null || query.getProject().equals(project))
                queries.add(query);
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (queries.size() <= offset) {
            offset = queries.size();
            limit = 0;
        }

        if ((queries.size() - offset) < limit) {
            limit = queries.size() - offset;
        }

        data.put("queries", queries.subList(offset, offset + limit));
        data.put("size", queries.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/query/format/{format}", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" }, consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void downloadQueryResultV2(@PathVariable String format, SQLRequest sqlRequest, HttpServletResponse response)
            throws IOException {

        SQLResponse result = queryService.doQueryWithCache(sqlRequest);

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
    public EnvelopeResponse getMetadataV2(MetaRequest metaRequest) throws SQLException, IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, queryService.getMetadataV2(metaRequest.getProject()),
                "");
    }

    public void setQueryService(KapQueryService queryService) {
        this.queryService = queryService;
    }
}
