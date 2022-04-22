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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import io.kyligence.kap.rest.request.AsyncQuerySQLRequest;
import io.kyligence.kap.rest.request.AsyncQuerySQLRequestV2;
import io.kyligence.kap.rest.response.AsyncQueryResponse;
import io.kyligence.kap.rest.response.AsyncQueryResponseV2;
import io.kyligence.kap.rest.service.AsyncQueryService;
import io.swagger.annotations.ApiOperation;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.query.exception.NAsyncQueryIllegalParamException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.util.List;


@RestController
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class NAsyncQueryControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("asyncQueryService")
    private AsyncQueryService asyncQueryService;

    @Autowired
    protected NAsyncQueryController asyncQueryController;

    @ApiOperation(value = "query", tags = {
            "QE" }, notes = "Update Param: query_id, accept_partial, backdoor_toggles, cache_key; Update Response: query_id")
    @PostMapping(value = "/async_query")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponseV2> query(@Valid @RequestBody final AsyncQuerySQLRequestV2 asyncQuerySQLRequest)
            throws InterruptedException, IOException {
        AsyncQuerySQLRequest sqlRequest = new AsyncQuerySQLRequest();
        sqlRequest.setProject(asyncQuerySQLRequest.getProject());
        sqlRequest.setSql(asyncQuerySQLRequest.getSql());
        sqlRequest.setSeparator(sqlRequest.getSeparator());
        sqlRequest.setFormat("csv");
        sqlRequest.setEncode("utf-8");
        sqlRequest.setFileName("result");
        sqlRequest.setLimit(asyncQuerySQLRequest.getLimit());
        sqlRequest.setOffset(asyncQuerySQLRequest.getOffset());

        AsyncQueryResponse resp = asyncQueryController.query(sqlRequest).getData();
        if (resp.getStatus() == AsyncQueryResponse.Status.RUNNING) {
            resp.setInfo("still running");
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, AsyncQueryResponseV2.from(resp), "");
    }


    @ApiOperation(value = "async query status", tags = { "QE" })
    @GetMapping(value = "/async_query/{query_id:.+}/metadata")
    @ResponseBody
    public EnvelopeResponse<List<List<String>>> metadata(@PathVariable("query_id") String queryId) throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, asyncQueryService.getMetaData(searchProject(queryId), queryId), "");
    }

    @ApiOperation(value = "fileStatus", tags = { "QE" }, notes = "Update URL: file_status")
    @GetMapping(value = "/async_query/{query_id:.+}/filestatus")
    @ResponseBody
    public EnvelopeResponse<Long> fileStatus(@PathVariable("query_id") String queryId) throws IOException {
        return asyncQueryController.fileStatus(queryId, null, searchProject(queryId));
    }



    @ApiOperation(value = "query", tags = { "QE" }, notes = "Update Response: query_id")
    @GetMapping(value = "/async_query/{query_id:.+}/status")
    @ResponseBody
    public EnvelopeResponse<AsyncQueryResponseV2> inqueryStatus(@PathVariable("query_id") String queryId)
            throws IOException {
        AsyncQueryResponse resp = asyncQueryController.inqueryStatus(null, queryId, searchProject(queryId)).getData();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, AsyncQueryResponseV2.from(resp), "");
    }

    @ApiOperation(value = "downloadQueryResult", tags = { "QE" }, notes = "Update URL: result")
    @GetMapping(value = "/async_query/{query_id:.+}/result_download")
    @ResponseBody
    public void downloadQueryResult(@PathVariable("query_id") String queryId,
                                    @RequestParam(value = "includeHeader", required = false, defaultValue = "false") boolean includeHeader,
                                    HttpServletResponse response) throws IOException {
        asyncQueryController.downloadQueryResult(queryId, includeHeader, includeHeader, null, response, searchProject(queryId));
    }

    private String searchProject(String queryId) throws IOException {
        String project = asyncQueryService.searchQueryResultProject(queryId);
        if (project == null) {
            throw new NAsyncQueryIllegalParamException(MsgPicker.getMsg().getQUERY_RESULT_NOT_FOUND());
        }
        return project;
    }

}
