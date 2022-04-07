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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.spark.sql.AnalysisException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.kyligence.kap.rest.request.DDLRequest;
import io.kyligence.kap.rest.request.ExportTableRequest;
import io.kyligence.kap.rest.response.DDLResponse;
import io.kyligence.kap.rest.response.ExportTablesResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.service.SparkSourceService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnProperty(name = "kylin.env.channel", havingValue = "cloud")
@RestController
@RequestMapping(value = "/api/spark_source", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON,
        HTTP_VND_APACHE_KYLIN_JSON })
@Slf4j
public class SparkSourceController extends NBasicController {

    @Autowired
    private SparkSourceService sparkSourceService;

    @ApiOperation(value = "execute", tags = { "DW" })
    @PostMapping(value = "/execute")
    @ResponseBody
    public EnvelopeResponse<DDLResponse> executeSQL(@RequestBody DDLRequest request) {
        DDLResponse ddlResponse = sparkSourceService.executeSQL(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, ddlResponse, "");
    }

    @ApiOperation(value = "exportTable", tags = { "DW" })
    @PostMapping(value = "/export_table_structure")
    @ResponseBody
    public EnvelopeResponse<ExportTablesResponse> exportTableStructure(@RequestBody ExportTableRequest request) {
        ExportTablesResponse tableResponse = sparkSourceService.exportTables(request.getDatabases(),
                request.getTables());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, tableResponse, "");
    }

    @ApiOperation(value = "dropTable", tags = { "DW" })
    @DeleteMapping(value = "/{database}/tables/{table}")
    public EnvelopeResponse<String> dropTable(@PathVariable("database") String database,
            @PathVariable("table") String table) throws AnalysisException {
        sparkSourceService.dropTable(database, table);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "listDatabase", tags = { "DW" })
    @GetMapping(value = "/databases")
    public EnvelopeResponse<List<String>> listDatabase() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listDatabase(), "");
    }

    @ApiOperation(value = "listTables", tags = { "DW" })
    @GetMapping(value = "/{database}/tables")
    public EnvelopeResponse<List<TableNameResponse>> listTables(@PathVariable("database") String database,
            @RequestParam("project") String project) throws Exception {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listTables(database, project), "");
    }

    @ApiOperation(value = "listColumns", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/columns")
    public EnvelopeResponse listColumns(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.listColumns(database, table), "");
    }

    @ApiOperation(value = "getTableDesc", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/desc")
    public EnvelopeResponse<String> getTableDesc(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.getTableDesc(database, table), "");
    }

    @ApiOperation(value = "hasPartition", tags = { "DW" })
    @GetMapping(value = "{database}/{table}/has_partition")
    public EnvelopeResponse<Boolean> hasPartition(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.hasPartition(database, table), "");
    }

    @ApiOperation(value = "databaseExists", tags = { "DW" })
    @GetMapping(value = "/{database}/exists")
    public EnvelopeResponse<Boolean> databaseExists(@PathVariable("database") String database) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.databaseExists(database), "");
    }

    @ApiOperation(value = "tableExists", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/exists")
    public EnvelopeResponse<Boolean> tableExists(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.tableExists(database, table), "");
    }

    @ApiOperation(value = "loadSamples", tags = { "DW" })
    @GetMapping(value = "/load_samples")
    public EnvelopeResponse<List<String>> loadSamples() throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.loadSamples(), "");
    }

    @ApiOperation(value = "msck", tags = { "DW" })
    @GetMapping(value = "/{database}/{table}/msck")
    public EnvelopeResponse<List<String>> msck(@PathVariable("database") String database,
            @PathVariable("table") String table) {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sparkSourceService.msck(database, table), "");
    }

}
