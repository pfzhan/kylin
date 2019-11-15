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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.rest.request.AsyncQuerySQLRequest;
import io.kyligence.kap.rest.response.AsyncQueryResponse;
import io.kyligence.kap.rest.service.AsyncQueryService;

@RestController
@RequestMapping(value = "/api")
public class NAsyncQueryController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NAsyncQueryController.class);

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    ExecutorService executorService = Executors.newCachedThreadPool();

    @RequestMapping(value = "/async_query", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse query(@RequestBody final AsyncQuerySQLRequest sqlRequest) throws InterruptedException {
        if (!KylinConfig.getInstanceFromEnv().getSchemaFactory()
                .equalsIgnoreCase("io.kyligence.kap.query.schema.KapSchemaFactory")) {
            throw new IllegalArgumentException("");
        }
        final AtomicReference<String> queryIdRef = new AtomicReference<>();
        final AtomicReference<Boolean> compileResultRef = new AtomicReference<>();
        final AtomicReference<String> exceptionHandle = new AtomicReference<>();
        final SecurityContext context = SecurityContextHolder.getContext();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                SecurityContextHolder.setContext(context);

                // TODO unsupport Sparder now
//                SparderEnv.setAsAsyncQuery();
//                SparderEnv.setSeparator(sqlRequest.getSeparator());
//                SparderEnv.setResultRef(compileResultRef);

                QueryContext queryContext = QueryContext.current();
                logger.info("Start a new async query with queryId: " + queryContext.getQueryId());
                String queryId = queryContext.getQueryId();
                queryIdRef.set(queryId);
                asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.RUNNING);
                try {
                    SQLResponse response = queryService.doQueryWithCache(sqlRequest, false);
                    if (response.isException()) {
                        asyncQueryService.createErrorFlag(response.getExceptionMessage(), queryContext.getQueryId());
                    }
                    asyncQueryService.updateStatus(queryId, AsyncQueryService.QueryStatus.SUCCESS);
                    asyncQueryService.saveMetaData(response, queryId);
                } catch (Exception e) {
                    try {
                        logger.error("failed to run query " + queryContext.getQueryId(), e);
                        compileResultRef.set(false);
                        asyncQueryService.createErrorFlag(e.getMessage(), queryContext.getQueryId());
                        exceptionHandle.set(e.getMessage());
                    } catch (Exception e1) {
                        exceptionHandle.set(exceptionHandle.get() + "\n" + e.getMessage());
                        throw new RuntimeException(e1);
                    }

                }
            }
        });

        while (compileResultRef.get() == null) {
            Thread.sleep(200);
        }
        if (compileResultRef.get()) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.RUNNING, "still running"), "");
        } else {
            //todo message
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                    new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.FAILED, exceptionHandle.get()), "");
        }

    }

    @RequestMapping(value = "/async_query", method = RequestMethod.DELETE, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse cleanAllQuery() throws IOException {
        Message msg = MsgPicker.getMsg();

        boolean result = asyncQueryService.cleanAllFolder();
        if (result)
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
        else
            throw new BadRequestException(msg.getCLEAN_FOLDER_FAIL());
    }

    @RequestMapping(value = "/async_query/{query_id}/status", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse inqueryStatus(@PathVariable("query_id") String query_id) throws IOException {

        AsyncQueryService.QueryStatus queryStatus = asyncQueryService.queryStatus(query_id);
        AsyncQueryResponse asyncQueryResponse = null;
        switch (queryStatus) {
            case SUCCESS:
                asyncQueryResponse = new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.SUCCESSFUL,
                        "await fetching results");
                break;
            case RUNNING:
                asyncQueryResponse = new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.RUNNING, "still running");
                break;
            case FAILED:
                asyncQueryResponse = new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.FAILED,
                        asyncQueryService.retrieveSavedQueryException(query_id));
                break;
            case MISS:
                asyncQueryResponse = new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.MISSING,
                        "query does not exit or got cleaned"); //
                break;
            default:
                throw new IllegalStateException("error queryStatus");
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, asyncQueryResponse, "");
    }

    @RequestMapping(value = "/async_query/{query_id}/filestatus", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse fileStatus(@PathVariable("query_id") String query_id) throws IOException {
        long length = asyncQueryService.fileStatus(query_id);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, length, "");
    }

    @RequestMapping(value = "/async_query/{query_id}/metadata", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse metadata(@PathVariable("query_id") String query_id) throws Exception {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, asyncQueryService.getMetaData(query_id), "");
    }

    @RequestMapping(value = "/async_query/{query_id}/result_download", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public void downloadQueryResult(@PathVariable("query_id") String query_id, HttpServletResponse response) throws IOException {
        KylinConfig config = queryService.getConfig();
        Message msg = MsgPicker.getMsg();

        if ((isAdmin() && !config.isAdminUserExportAllowed())
                || (!isAdmin() && !config.isNoneAdminUserExportAllowed())) {
            throw new ForbiddenException(msg.getEXPORT_RESULT_NOT_ALLOWED());
        }

        response.setContentType("text/csv;charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"result.csv\"");

        asyncQueryService.retrieveSavedQueryResult(query_id, response);
    }

    @RequestMapping(value = "/async_query/{query_id}/result_path", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse queryPath(@PathVariable("query_id") String query_id, HttpServletResponse response) throws IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, asyncQueryService.asyncQueryResultPath(query_id), "");
    }

}
