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

import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.FactTableRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Controller
@RequestMapping(value = "/tables")
@Component("TableController")
public class NTableController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NTableController.class);

    private static final Message msg = MsgPicker.getMsg();

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("tableExtService")
    private TableExtService tableExtService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTableDesc(@RequestParam(value = "ext", required = false) boolean withExt,
            @RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "table", required = false) String table) throws IOException {

        checkProjectName(project);
        List<TableDesc> tableDescs = new ArrayList<>();
        if (StringUtils.isEmpty(table)) {
            tableDescs.addAll(tableService.getTableDesc(project, withExt));

        } else {
            tableDescs.add(tableService.getTableDescByName(table, withExt, project));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, tableDescs, "");
    }

    /**
     * set table fact
     *
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/fact", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setTableFact(@RequestBody FactTableRequest factTableRequest) throws IOException {

        checkProjectName(factTableRequest.getProject());
        checkRequiredArg("column", factTableRequest.getColumn());
        tableService.setFact(factTableRequest.getTable(), factTableRequest.getProject(), factTableRequest.getFact(),
                factTableRequest.getColumn());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse loadTables(@RequestBody TableLoadRequest tableLoadRequest) throws Exception {

        Set<String>[] sets;
        checkProjectName(tableLoadRequest.getProject());
        if (tableLoadRequest.getTables() == null || tableLoadRequest.getTables().length == 0) {
            throw new BadRequestException("you should select at least 1 table to load");
        }
        sets = tableExtService.loadTables(tableLoadRequest.getTables(), tableLoadRequest.getProject(),
                tableLoadRequest.getDatasourceType());

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, sets, "");
    }

    @RequestMapping(value = "/date_range", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setDateRanges(@RequestBody DateRangeRequest dateRangeRequest) throws Exception {
        checkProjectName(dateRangeRequest.getProject());
        checkRequiredArg("table", dateRangeRequest.getTable());
        if (dateRangeRequest.getStartTime() >= dateRangeRequest.getEndTime()) {
            throw new BadRequestException("Illegal starttime or endtime,endTime"+dateRangeRequest.getEndTime()+"must be larger than startTime"+dateRangeRequest.getStartTime());
        }
        if(dateRangeRequest.getStartTime() < 0){
            throw new BadRequestException("Illegal starttime ,startTime"+dateRangeRequest.getStartTime()+"can not be negative");

        }
        tableService.setDataRange(dateRangeRequest.getProject(), dateRangeRequest.getTable(),
                dateRangeRequest.getStartTime(), dateRangeRequest.getEndTime());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/databases", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showDatabases(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "datasourceType", required = true) Integer datasourceType) throws Exception {

        checkProjectName(project);
        List<String> databases = tableService.getSourceDbNames(project, datasourceType);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, databases, "");
    }

    /**
     * Show all tablesNames
     *
     * @return String[]
     * @throws IOException
     */
    @RequestMapping(value = "/names", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse showTables(@RequestParam(value = "project", required = true) String project,
            @RequestParam(value = "datasourceType", required = true) Integer dataSourceType,
            @RequestParam(value = "database", required = true) String database) throws Exception {

        checkProjectName(project);
        List<String> tables = tableService.getSourceTableNames(project, database, dataSourceType);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, tables, "");
    }

}
