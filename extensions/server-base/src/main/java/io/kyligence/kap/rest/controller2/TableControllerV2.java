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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.HiveTableRequestV2;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.TableACLService;
import org.apache.kylin.rest.service.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.response.HiveResponse;
import io.kyligence.kap.rest.service.ColumnACLService;
import io.kyligence.kap.rest.service.RowACLService;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/tables")
public class TableControllerV2 extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TableControllerV2.class);

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableACLService;

    @Autowired
    @Qualifier("ColumnAclService")
    private ColumnACLService columnACLService;

    @Autowired
    @Qualifier("RowAclService")
    private RowACLService rowACLService;

    /**
     * Get available table list of the project
     *
     * @return Table metadata array
     * @throws IOException
     */

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDescV2(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project", required = true) String project) throws IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableService.getTableDescByProject(project, withExt),
                "");
    }

    // FIXME prj-table
    /**
     * Get available table list of the input database
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/{project}/{tableName:.+}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDescV2(@PathVariable String tableName, @PathVariable String project) {
        Message msg = MsgPicker.getMsg();

        TableDesc table = tableService.getTableDescByName(tableName, false, project);
        if (table == null)
            throw new BadRequestException(String.format(msg.getHIVE_TABLE_NOT_FOUND(), tableName));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, table, "");
    }

    @RequestMapping(value = "/load", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadHiveTablesV2(@RequestBody HiveTableRequestV2 requestV2) throws Exception {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                tableService.loadHiveTables(requestV2.getTables(), requestV2.getProject(), requestV2.isNeedProfile()),
                "");
    }

    @RequestMapping(value = "/load", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse unLoadHiveTablesV2(@RequestBody HiveTableRequestV2 requestV2) throws IOException {
        for (String table : requestV2.getTables()) {
            delLowLevelACL(requestV2.getProject(), table);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                tableService.unloadHiveTables(requestV2.getTables(), requestV2.getProject()), "");
    }

    // FIXME prj-table
    /**
     * Regenerate table cardinality
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "/cardinality", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void generateCardinalityV2(@RequestBody HiveTableRequestV2 requestV2) throws Exception {

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        String[] tables = requestV2.getTables();
        String project = requestV2.getProject();

        for (String table : tables) {
            tableService.calculateCardinality(table.toUpperCase(), submitter, project);
        }
    }

    /**
     * Show all databases in Hive
     *
     * @return Hive databases list
     * @throws IOException
     */

    @RequestMapping(value = "/databases", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    private EnvelopeResponse showHiveDatabasesV2() throws Exception {
        KapMessage msg = KapMsgPicker.getMsg();
        List<String> databases = null;

        try {
            databases = tableService.getHiveDbNames();
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new BadRequestException(msg.getHIVE_TABLE_LOAD_FAILED());
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, databases, "");
    }

    /**
     * Show all tables in a Hive database
     *
     * @return Hive table list
     * @throws IOException
     */

    @RequestMapping(value = "/hive/{database}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    private EnvelopeResponse showHiveTablesV2(@PathVariable String database) throws Exception {
        KapMessage msg = KapMsgPicker.getMsg();
        List<String> tables = null;

        try {
            tables = tableService.getHiveTableNames(database);
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new BadRequestException(msg.getHIVE_TABLE_LOAD_FAILED());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tables, "");
    }

    /**
     * Show all databases and tables in Hive
     *
     * @return Hive databases list with Hive table list
     * @throws IOException
     */
    @RequestMapping(value = "/hive", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    private EnvelopeResponse showHiveDatabasesAndTables() throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();
        List<HiveResponse> hive = new ArrayList<HiveResponse>();

        try {
            List<String> dataBases = tableService.getHiveDbNames();

            for (String dataBase : dataBases) {
                List<String> tableNames = tableService.getHiveTableNames(dataBase);
                hive.add(new HiveResponse(dataBase, tableNames));
            }
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new BadRequestException(msg.getHIVE_TABLE_LOAD_FAILED());
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, hive, "");
    }

    private void delLowLevelACL(String project, String table) throws IOException {
        tableACLService.deleteFromTableBlackListByTbl(project, table);
        columnACLService.deleteFromTableBlackListByTbl(project, table);
        rowACLService.deleteFromRowCondListByTbl(project, table);
    }
}
