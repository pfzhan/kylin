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

package io.kyligence.kap.rest.controller.v2;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.List;

import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.rest.controller.NBasicController;
import io.swagger.annotations.ApiOperation;

/**
 * Backward capable API for KyligenceODBC: /kylin/api/tables_and_columns
 *
 * Ref(KE 3.x): org.apache.kylin.rest.controller.QueryController.getMetadata(MetaRequest)
 *
 * TODO ODBC should support Newten API: /kylin/api/query/tables_and_columns
 *
 * @author yifanzhang
 *
 */
@RestController
@RequestMapping(value = "/api")
@Deprecated
public class NQueryMetaController extends NBasicController {

    @Autowired
    private QueryService queryService;

    @ApiOperation(value = "getMetadataForDriver", tags = { "QE" })
    @GetMapping(value = "/tables_and_columns", produces = { "application/json", HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    @Deprecated
    public List<TableMeta> getMetadataForDriver(MetaRequest metaRequest) {
        if (metaRequest.getCube() == null) {
            return queryService.getMetadata(metaRequest.getProject());
        } else {
            return queryService.getMetadata(metaRequest.getProject(), metaRequest.getCube());
        }
    }
}
