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
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_ITEM_ID_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_PROJECT_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_AND_SQL_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_EXISTS;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_SQL_EXISTS;

import com.google.common.collect.Sets;
import io.kyligence.kap.rest.service.QuerySQLBlacklistService;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/api/query_sql_blacklist", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON, HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class QuerySQLBlacklistController extends NBasicController {

    @Autowired
    @Qualifier("querySQLBlacklistService")
    private QuerySQLBlacklistService querySQLBlacklistService;

    @ApiOperation(value = "getSqlBlacklist", tags = { "QE" })
    @GetMapping(value = "/{project}")
    @ResponseBody
    public EnvelopeResponse getSqlBlacklist(@PathVariable(value = "project") String project) {
        Message msg = Message.getInstance();
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSQL_BLACKLIST_ITEM_PROJECT_EMPTY());
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.getSqlBlacklist(project);
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "overwrite", tags = { "QE" })
    @PostMapping(value = "/overwrite")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> overwrite(@RequestBody SQLBlacklistRequest sqlBlacklistRequest) throws IOException {
         Message msg = Message.getInstance();
        if (null == sqlBlacklistRequest.getProject()) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSQL_BLACKLIST_ITEM_PROJECT_EMPTY());
        }
        validateSqlBlacklist(sqlBlacklistRequest.getBlacklistItems());
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.saveSqlBlacklist(sqlBlacklistRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    private void validateSqlBlacklist(List<SQLBlacklistItemRequest> sqlBlacklistItemRequests) {
         Message msg = Message.getInstance();

        Set<String> regexSet = Sets.newHashSet();
        Set<String> sqlSet = Sets.newHashSet();
        for (SQLBlacklistItemRequest item : sqlBlacklistItemRequests) {
            if (null == item.getRegex() && null == item.getSql()) {
                throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSQL_BLACKLIST_ITEM_REGEX_AND_SQL_EMPTY());
            }
            String regex = item.getRegex();
            if (null != regex && regexSet.contains(regex)) {
                throw new KylinException(BLACKLIST_REGEX_EXISTS, msg.getSQL_BLACKLIST_ITEM_REGEX_EXISTS());
            }
            String sql = item.getSql();
            if (null != sql && sqlSet.contains(sql)) {
                throw new KylinException(BLACKLIST_SQL_EXISTS, msg.getSQL_BLACKLIST_ITEM_SQL_EXISTS());
            }
            if (null != regex) {
                regexSet.add(item.getRegex());
            }
            if (null != sql) {
                sqlSet.add(item.getSql());
            }
        }
    }

    @ApiOperation(value = "add_item", tags = { "QE" })
    @PostMapping(value = "/add_item/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> addItem(@PathVariable(value = "project") String project,
            @RequestBody SQLBlacklistItemRequest sqlBlacklistItemRequest) throws IOException {
         Message msg = Message.getInstance();
        if (null == sqlBlacklistItemRequest.getRegex() && null == sqlBlacklistItemRequest.getSql()) {
            throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSQL_BLACKLIST_ITEM_REGEX_AND_SQL_EMPTY());
        }
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSQL_BLACKLIST_ITEM_PROJECT_EMPTY());
        }
        SQLBlacklistItem sqlBlacklistItemOfRegex = querySQLBlacklistService.getItemByRegex(project,
                sqlBlacklistItemRequest);
        if (null != sqlBlacklistItemOfRegex) {
            throw new KylinException(BLACKLIST_REGEX_EMPTY, String.format(Locale.ROOT, msg.getSQL_BLACKLIST_ITEM_REGEX_EXISTS(), sqlBlacklistItemOfRegex.getId()));
        }
        SQLBlacklistItem sqlBlacklistItemOfSql = querySQLBlacklistService.getItemBySql(project,
                sqlBlacklistItemRequest);
        if (null != sqlBlacklistItemOfSql) {
            throw new KylinException(BLACKLIST_SQL_EXISTS, String.format(Locale.ROOT, msg.getSQL_BLACKLIST_ITEM_SQL_EXISTS(), sqlBlacklistItemOfSql.getId()));
        }

        SQLBlacklist sqlBlacklist = querySQLBlacklistService.addSqlBlacklistItem(project, sqlBlacklistItemRequest);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "update_item", tags = { "QE" })
    @PostMapping(value = "/update_item/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> updateItem(@PathVariable(value = "project") String project,
            @RequestBody SQLBlacklistItemRequest sqlBlacklistItemRequest) throws IOException {
         Message msg = Message.getInstance();
        if (null == sqlBlacklistItemRequest.getId()) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSQL_BLACKLIST_ITEM_ID_EMPTY());
        }
        if (null == sqlBlacklistItemRequest.getRegex() && null == sqlBlacklistItemRequest.getSql()) {
            throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSQL_BLACKLIST_ITEM_REGEX_AND_SQL_EMPTY());
        }
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSQL_BLACKLIST_ITEM_PROJECT_EMPTY());
        }
        if (null == querySQLBlacklistService.getItemById(project, sqlBlacklistItemRequest)) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSQL_BLACKLIST_ITEM_ID_NOT_EXISTS());
        }
        SQLBlacklistItem conflictRegexItem = querySQLBlacklistService.checkConflictRegex(project,
                sqlBlacklistItemRequest);
        if (null != conflictRegexItem) {
            throw new KylinException(BLACKLIST_REGEX_EMPTY, String.format(Locale.ROOT, msg.getSQL_BLACKLIST_ITEM_REGEX_EXISTS(), conflictRegexItem.getId()));
        }
        SQLBlacklistItem conflictSqlItem = querySQLBlacklistService.checkConflictSql(project,
                sqlBlacklistItemRequest);
        if (null != conflictSqlItem) {
            throw new KylinException(BLACKLIST_SQL_EXISTS, String.format(Locale.ROOT, msg.getSQL_BLACKLIST_ITEM_SQL_EXISTS(), conflictSqlItem.getId()));
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.updateSqlBlacklistItem(project,
                sqlBlacklistItemRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "delete_item", tags = { "QE" })
    @DeleteMapping(value = "/delete_item/{project}/{id}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> deleteItem(@PathVariable(value = "project") String project,
                                       @PathVariable(value = "id") String id) throws IOException {
        Message msg = Message.getInstance();
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSQL_BLACKLIST_ITEM_PROJECT_EMPTY());
        }
        if (null == id) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSQL_BLACKLIST_ITEM_ID_TO_DELETE_EMPTY());
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.deleteSqlBlacklistItem(project, id);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "clearBlacklist", tags = { "QE" })
    @DeleteMapping(value = "/clear/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> clearBlacklist(@PathVariable String project) throws IOException {
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.clearSqlBlacklist(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }
}
