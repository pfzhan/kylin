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

import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.request.EpochRequest;
import io.kyligence.kap.rest.service.EpochService;

@Controller
@RequestMapping(value = "/api/epoch", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NEpochController extends NBasicController {

    @Autowired
    @Qualifier("epochService")
    private EpochService epochService;

    @PostMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateEpochOwner(@RequestBody EpochRequest epochRequest) throws Exception {
        epochService.updateEpoch(epochRequest.getProjects(), epochRequest.isForce(), epochRequest.isClient());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/all", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> updateAllEpochOwner(@RequestBody EpochRequest epochRequest) throws Exception {
        epochService.updateAllEpochs(epochRequest.isForce(), epochRequest.isClient());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "");
    }
}