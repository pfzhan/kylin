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

package io.kyligence.kap.rest.controllerV2;

import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.service.MassInService;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

@Controller
@RequestMapping(value = "/massin")
public class MassInControllerV2 extends BasicController{
    @Autowired
    private MassInService massInService;

    @Autowired
    @Qualifier("queryServiceV2")
    private QueryServiceV2 queryServiceV2;

    private static final Logger logger = LoggerFactory.getLogger(MassInControllerV2.class);

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse queryMassin(@RequestHeader("Accept-Language") String lang, @RequestBody SQLRequest sqlRequest) throws IOException {
        KapMsgPicker.setMsg(lang);

        SQLResponse queryResult = queryServiceV2.doQueryWithCache(sqlRequest);
        String resourcePath = massInService.storeMassIn(queryResult, Functions.FilterTableType.HDFS);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, resourcePath, "");
    }
}
