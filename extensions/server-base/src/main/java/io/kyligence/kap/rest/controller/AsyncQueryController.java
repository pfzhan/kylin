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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.response.AsyncQueryResponse;
import io.kyligence.kap.rest.service.AsyncQueryService;

@Controller
@EnableAsync
public class AsyncQueryController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryController.class);

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    ExecutorService executorService = Executors.newCachedThreadPool();

    @RequestMapping(value = "/async_query", method = RequestMethod.POST, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse query(@RequestBody final SQLRequest sqlRequest) throws InterruptedException {

        final AtomicReference<String> queryIdRef = new AtomicReference<>();
        final SecurityContext context = SecurityContextHolder.getContext();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                SecurityContextHolder.setContext(context);
                QueryContext queryContext = QueryContext.current();
                logger.info("Start a new async query with queryId: " + queryContext.getQueryId());
                queryIdRef.set(queryContext.getQueryId());
                try {
                    asyncQueryService.createExistFlag(queryContext.getQueryId());
                    try {
                        SQLResponse response = queryService.doQueryWithCache(sqlRequest);
                        asyncQueryService.flushResultToHdfs(response, queryContext.getQueryId());
                    } catch (Exception ie) {
                        SQLResponse error = new SQLResponse(null, null, null, 0, true, ie.getMessage(), false, false);
                        asyncQueryService.flushResultToHdfs(error, queryContext.getQueryId());
                    }
                } catch (IOException e) {
                    logger.error("failed to run query " + queryContext.getQueryId(), e);
                }
            }
        });

        while (StringUtils.isEmpty(queryIdRef.get())) {
            Thread.sleep(1);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                new AsyncQueryResponse(queryIdRef.get(), AsyncQueryResponse.Status.RUNNING, "still running"), "");
    }

    @RequestMapping(value = "/async_query", method = RequestMethod.DELETE, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse clean() throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        boolean b = asyncQueryService.cleanFolder();
        if (b)
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, b, "");
        else
            throw new BadRequestException(msg.getCLEAN_FOLDER_FAIL());
    }

    @RequestMapping(value = "/async_query/{query_id}/status", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse inqueryStatus(@PathVariable String query_id) throws IOException {

        if (asyncQueryService.isQueryFailed(query_id)) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, //
                    new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.FAILED,
                            asyncQueryService.retrieveSavedQueryException(query_id)), //
                    "");
        } else if (asyncQueryService.isQuerySuccessful(query_id)) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, //
                    new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.SUCCESSFUL, "await fetching results"), //
                    "");
        } else if (asyncQueryService.isQueryExisting(query_id)) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, //
                    new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.RUNNING, "still running"), //
                    "");
        } else {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, //
                    new AsyncQueryResponse(query_id, AsyncQueryResponse.Status.MISSING,
                            "query does not exit or got cleaned"), //
                    "");
        }
    }

    @RequestMapping(value = "/async_query/{query_id}/result", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void downloadQueryResult(@PathVariable String query_id, HttpServletResponse response) throws IOException {

        response.setContentType("text/csv;charset=utf-8");
        response.setHeader("Content-Disposition", "attachment; filename=\"result.csv\"");

        asyncQueryService.retrieveSavedQueryResult(query_id, response);
    }

}
