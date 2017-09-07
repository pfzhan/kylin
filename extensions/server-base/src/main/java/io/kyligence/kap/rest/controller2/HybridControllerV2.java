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

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.HybridRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.HybridService;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/hybrids")
public class HybridControllerV2 extends BasicController {

    @Autowired
    private HybridService hybridService;

    @RequestMapping(value = "", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse createV2(@RequestBody HybridRequest request) {

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance instance = hybridService.createHybridCube(request.getHybrid(), request.getProject(),
                request.getModel(), request.getCubes());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, instance, "");
    }

    @RequestMapping(value = "", method = RequestMethod.PUT, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateV2(@RequestBody HybridRequest request) {

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance instance = hybridService.updateHybridCube(request.getHybrid(), request.getProject(),
                request.getModel(), request.getCubes());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, instance, "");
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteV2(@RequestBody HybridRequest request) {

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        hybridService.deleteHybridCube(request.getHybrid(), request.getProject(), request.getModel());
    }

    @RequestMapping(value = "", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listV2(@RequestParam(required = false) String project,
            @RequestParam(required = false) String model) {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hybridService.listHybrids(project, model), "");
    }

    @RequestMapping(value = "{hybrid}", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getV2(@PathVariable String hybrid) {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hybridService.getHybridInstance(hybrid), "");
    }
}
