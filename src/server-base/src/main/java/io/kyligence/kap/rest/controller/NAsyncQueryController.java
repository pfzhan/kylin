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

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.query.exception.QueryErrorCode;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.AsyncQuerySQLRequest;
import io.kyligence.kap.rest.response.AsyncQueryResponse;
import io.kyligence.kap.rest.service.AsyncQueryService;
import io.kyligence.kap.rest.service.KapQueryService;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NAsyncQueryController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NAsyncQueryController.class);

    private static final List<String> FILE_ENCODING=Lists.newArrayList("utf-8", "gbk");
    private static final List<String> FILE_FORMAT=Lists.newArrayList("csv", "json", "xlsx");

    @Autowired
    @Qualifier("kapQueryService")
    private KapQueryService queryService;

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    ExecutorService executorService = Executors.newCachedThreadPool();

    @ApiOperation(value = "query", notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key; Update Response: query_id")
    @PostMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponse> query(@Valid @RequestBody final AsyncQuerySQLRequest sqlRequest)
            throws InterruptedException, IOException {
        checkProjectName(sqlRequest.getProject());
        if (!FILE_ENCODING.contains(sqlRequest.getEncode().toLowerCase())) {
            return new EnvelopeResponse<>(QueryErrorCode.ASYNC_QUERY_ILLEGAL_PARAM.toErrorCode().getString(),
                    new AsyncQueryResponse(sqlRequest.getQueryId(), AsyncQueryResponse.Status.FAILED,
                    "Format " + sqlRequest.getFormat() + " unsupported. Only " + FILE_FORMAT + " are supported"),
                    "");
        }
        if (!FILE_FORMAT.contains(sqlRequest.getFormat().toLowerCase())) {
            return new EnvelopeResponse<>(QueryErrorCode.ASYNC_QUERY_ILLEGAL_PARAM.toErrorCode().getString(),
                    new AsyncQueryResponse(sqlRequest.getQueryId(), AsyncQueryResponse.Status.FAILED,
                            "Format " + sqlRequest.getFormat() + " unsupported. Only " + FILE_FORMAT + " are supported"),
                    "");
        }
        final AtomicReference<String> queryIdRef = new AtomicReference<>();
        final AtomicReference<Boolean> compileResultRef = new AtomicReference<>();
        final AtomicReference<String> exceptionHandle = new AtomicReference<>();
        final SecurityContext context = SecurityContextHolder.getContext();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                String format = sqlRequest.getFormat().toLowerCase();
                String encode = sqlRequest.getEncode().toLowerCase();
                SecurityContextHolder.setContext(context);

                SparderEnv.setSeparator(sqlRequest.getSeparator());
                SparderEnv.setResultRef(compileResultRef);

                QueryContext queryContext = QueryContext.current();
                sqlRequest.setQueryId(queryContext.getQueryId());
                queryContext.getQueryTagInfo().setAsyncQuery(true);
                queryContext.getQueryTagInfo().setFileFormat(format);
                queryContext.getQueryTagInfo().setFileEncode(encode);
                queryContext.setProject(sqlRequest.getProject());
                logger.info("Start a new async query with queryId: " + queryContext.getQueryId());
                String queryId = queryContext.getQueryId();
                queryIdRef.set(queryId);
                try {
                    SQLResponse response = queryService.doQueryWithCache(sqlRequest, false);
                    if (response.isException()) {
                        asyncQueryService.createErrorFlag(sqlRequest.getProject(), queryContext.getQueryId(),
                                response.getExceptionMessage());
                        compileResultRef.set(false);
                        exceptionHandle.set(response.getExceptionMessage());
                    } else {
                        asyncQueryService.saveMetaData(sqlRequest.getProject(), response, queryId);
                        asyncQueryService.saveFileInfo(sqlRequest.getProject(), format, encode, sqlRequest.getFileName(), queryContext.getQueryId());
                        compileResultRef.set(true);
                    }
                    asyncQueryService.saveQueryUsername(sqlRequest.getProject(), queryId);
                } catch (Exception e) {
                    try {
                        logger.error("failed to run query " + queryContext.getQueryId(), e);
                        compileResultRef.set(false);
                        asyncQueryService.createErrorFlag(sqlRequest.getProject(), queryContext.getQueryId(),
                                e.getMessage());
                        exceptionHandle.set(e.getMessage());
                    } catch (Exception e1) {
                        exceptionHandle.set(exceptionHandle.get() + "\n" + e.getMessage());
                        throw new RuntimeException(e1);
                    }
                } finally {
                    QueryContext.current().close();
                }
            }
        });

        while (compileResultRef.get() == null) {
            Thread.sleep(200);
        }

        switch (asyncQueryService.queryStatus(sqlRequest.getProject(), sqlRequest.getQueryId())) {
        case SUCCESS:
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.SUCCESSFUL, "query success"),
                    "");
        case FAILED:
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.FAILED, exceptionHandle.get()),
                    "");
        default:
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.RUNNING, "query still running"),
                    "");
        }
    }

    @DeleteMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<Boolean> batchDelete(@RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "older_than", required = false) String time) throws Exception {
        if (!isAdmin()) {
            return new EnvelopeResponse<Boolean>(ResponseCode.CODE_UNAUTHORIZED, false,
                    "Access denied. Only admin users can delete the query results");
        }
        Message msg = MsgPicker.getMsg();
        if (asyncQueryService.batchDelete(project, time))
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
        else
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, false, msg.getCLEAN_FOLDER_FAIL());
    }

    @DeleteMapping(value = "/async_query/{query_id:.+}")
    @ResponseBody
    public EnvelopeResponse<Boolean> deleteByQueryId(@PathVariable("query_id") String queryId,
            @Valid @RequestBody final AsyncQuerySQLRequest sqlRequest) throws IOException {
        checkProjectName(sqlRequest.getProject());
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, false,
                    "Access denied. Only task submitters or admin users can delete the query results");
        }
        Message msg = MsgPicker.getMsg();
        boolean result = asyncQueryService.deleteByQueryId(sqlRequest.getProject(), queryId);
        if (result)
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, true, "");
        else
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, false, msg.getCLEAN_FOLDER_FAIL());
    }

    @ApiOperation(value = "query", notes = "Update Response: query_id")
    @GetMapping(value = "/async_query/{query_id:.+}/status")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponse> inqueryStatus(@Valid @RequestBody final AsyncQuerySQLRequest sqlRequest,
            @PathVariable("query_id") String queryId) throws IOException {
        checkProjectName(sqlRequest.getProject());
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, null,
                    "Access denied. Only task submitters or admin users can get the query status");
        }
        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(sqlRequest.getProject(), queryId);
        AsyncQueryResponse asyncQueryResponse;
        switch (queryStatus) {
        case SUCCESS:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.SUCCESSFUL,
                    "await fetching results");
            break;
        case RUNNING:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.RUNNING, "still running");
            break;
        case FAILED:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.FAILED,
                    asyncQueryService.retrieveSavedQueryException(sqlRequest.getProject(), queryId));
            break;
        default:
            asyncQueryResponse = new AsyncQueryResponse(queryId, AsyncQueryResponse.Status.MISSING,
                    "query status is lost"); //
            break;
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, asyncQueryResponse, "");
    }

    @ApiOperation(value = "fileStatus", notes = "Update URL: file_status")
    @GetMapping(value = "/async_query/{query_id:.+}/file_status")
    @ResponseBody
    public EnvelopeResponse<Long> fileStatus(@PathVariable("query_id") String queryId,
            @Valid @RequestBody final AsyncQuerySQLRequest sqlRequest) throws IOException {
        checkProjectName(sqlRequest.getProject());
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, 0L,
                    "Access denied. Only task submitters or admin users can get the file status");
        }
        long length = asyncQueryService.fileStatus(sqlRequest.getProject(), queryId);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, length, "");
    }

    @GetMapping(value = "/async_query/{query_id:.+}/metadata")
    @ResponseBody
    public EnvelopeResponse<List<List<String>>> metadata(@Valid @RequestBody final AsyncQuerySQLRequest sqlRequest,
            @PathVariable("query_id") String queryId) throws IOException {
        checkProjectName(sqlRequest.getProject());
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, null,
                    "Access denied. Only task submitters or admin users can get the metadata");
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                asyncQueryService.getMetaData(sqlRequest.getProject(), queryId), "");
    }

    @ApiOperation(value = "downloadQueryResult", notes = "Update URL: result")
    @GetMapping(value = "/async_query/{query_id:.+}/result_download")
    @ResponseBody
    public EnvelopeResponse<String> downloadQueryResult(@PathVariable("query_id") String queryId,
            @RequestParam(value = "include_header", required = false, defaultValue = "false") boolean include_header,
            @RequestParam(value = "includeHeader", required = false, defaultValue = "false") boolean includeHeader,
            @Valid @RequestBody final AsyncQuerySQLRequest sqlRequest, HttpServletResponse response)
            throws IOException {
        checkProjectName(sqlRequest.getProject());
        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, "",
                    "Access denied. Only task submitters or admin users can download the query results");
        }
        if (((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed()))) {
            throw new ForbiddenException(msg.getEXPORT_RESULT_NOT_ALLOWED());
        }
        AsyncQueryService.FileInfo fileInfo = asyncQueryService.getFileInfo(sqlRequest.getProject(), queryId);
        String format = fileInfo.getFormat();
        String encode = fileInfo.getEncode();
        String fileName = fileInfo.getFileName();
        if (format.equals("xlsx")) {
            response.setContentType("application/octet-stream;charset=" + encode);
        } else {
            response.setContentType("application/" + format + ";charset=" + encode);
        }
        response.setHeader("Content-Disposition", "attachment; filename=\"" + fileName + "." + format + "\"");
        asyncQueryService.retrieveSavedQueryResult(sqlRequest.getProject(), queryId, includeHeader || include_header, response, format, encode);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/async_query/{query_id:.+}/result_path")
    @ResponseBody
    public EnvelopeResponse<String> queryPath(@PathVariable("query_id") String queryId,
            @Valid @RequestBody final AsyncQuerySQLRequest sqlRequest, HttpServletResponse response)
            throws IOException {
        checkProjectName(sqlRequest.getProject());
        if (!asyncQueryService.hasPermission(queryId, sqlRequest.getProject())) {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNAUTHORIZED, "",
                    "Access denied. Only task submitters or admin users can get the query path");
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                asyncQueryService.asyncQueryResultPath(sqlRequest.getProject(), queryId), "");
    }
}
