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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.service.SQLValidateService;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

@Controller
@RequestMapping(value = "/sql_validate")
public class SQLValidateController extends BasicController {
    @Autowired
    @Qualifier("sqlValidateService")
    private SQLValidateService sqlValidateService;

    @RequestMapping(value = "/cube", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse validateCubeSQL(@RequestBody SQLValidateRequest validateRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        if (validateRequest == null) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, null, msg.getSQL_VALIDATE_FAILED());
        }
        List<SQLValidateResult> validateStats = sqlValidateService.validateCubeSQL(validateRequest.getSqls(),
                validateRequest.getCubeName());
        return new EnvelopeResponse(
                CollectionUtils.isNotEmpty(validateStats) ? ResponseCode.CODE_SUCCESS : ResponseCode.CODE_UNDEFINED,
                validateStats, msg.getSQL_VALIDATE_FAILED());
    }

    @RequestMapping(value = "/model", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse validateModelSQL(@RequestBody SQLValidateRequest validateRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        if (validateRequest == null) {
            return new EnvelopeResponse(ResponseCode.CODE_UNDEFINED, null, msg.getSQL_VALIDATE_FAILED());
        }
        List<SQLValidateResult> validateStats = sqlValidateService.validateModelSQL(validateRequest.getSqls(),
                validateRequest.getModelName());
        return new EnvelopeResponse(
                CollectionUtils.isNotEmpty(validateStats) ? ResponseCode.CODE_SUCCESS : ResponseCode.CODE_UNDEFINED,
                validateStats, msg.getSQL_VALIDATE_FAILED());
    }
}
