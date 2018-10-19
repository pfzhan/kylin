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

import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.rest.request.CreateTableIndexRequest;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.service.CubePlanService;

@RestController
@RequestMapping(value = "/cube_plans")
public class NCubePlanController extends NBasicController {

    private static final String CUBE_PLAN_NAME = "cubePlanName";

    @Autowired
    @Qualifier("cubePlanService")
    private CubePlanService cubePlanService;

    @PutMapping(value = "/rule", produces = {"application/vnd.apache.kylin-v2+json"})
    public EnvelopeResponse updateRule(@RequestBody UpdateRuleBasedCuboidRequest request) throws IOException, PersistentException {
        checkProjectName(request.getProject());
        checkRequiredArg(CUBE_PLAN_NAME, request.getCubePlanName());
        cubePlanService.updateRuleBasedCuboid(request);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @PostMapping(value = "/table_index", produces = {"application/vnd.apache.kylin-v2+json"})
    public EnvelopeResponse createTableIndex(@RequestBody CreateTableIndexRequest request) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @DeleteMapping(value = "/table_index", produces = {"application/vnd.apache.kylin-v2+json"})
    public EnvelopeResponse deleteTableIndex() {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

}
