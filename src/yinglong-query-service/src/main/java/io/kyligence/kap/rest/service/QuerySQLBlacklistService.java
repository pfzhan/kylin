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

package io.kyligence.kap.rest.service;

import com.google.common.collect.Lists;
import io.kyligence.kap.rest.aspect.Transaction;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.query.blacklist.SQLBlacklistManager;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

@Component("querySQLBlacklistService")
public class QuerySQLBlacklistService extends BasicService {

    private SQLBlacklistManager getSQLBlacklistManager() {
        return SQLBlacklistManager.getInstance(getConfig());
    }

    public SQLBlacklist getSqlBlacklist(String project) {
        return getSQLBlacklistManager().getSqlBlacklist(project);
    }

    @Transaction(project = 0)
    public SQLBlacklist saveSqlBlacklist(SQLBlacklistRequest sqlBlacklistRequest) throws IOException {
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        sqlBlacklist.setProject(sqlBlacklistRequest.getProject());
        List<SQLBlacklistItemRequest> itemRequestList = sqlBlacklistRequest.getBlacklistItems();
        List<SQLBlacklistItem> itemList = Lists.newArrayList();
        if (null != itemRequestList) {
            for (SQLBlacklistItemRequest itemRequest : itemRequestList) {
                SQLBlacklistItem item = new SQLBlacklistItem();
                item.updateRandomUuid();
                item.setSql(itemRequest.getSql());
                item.setRegex(itemRequest.getRegex());
                item.setConcurrentLimit(itemRequest.getConcurrentLimit());
                itemList.add(item);
            }
        }
        sqlBlacklist.setBlacklistItems(itemList);
        return getSQLBlacklistManager().saveSqlBlacklist(sqlBlacklist);
    }

    public SQLBlacklistItem getItemById(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        return getSQLBlacklistManager().getSqlBlacklistItemById(project, sqlBlacklistItemRequest.getId());
    }

    public SQLBlacklistItem getItemByRegex(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String regex = sqlBlacklistItemRequest.getRegex();
        if (null == regex) {
            return null;
        }
        return getSQLBlacklistManager().getSqlBlacklistItemByRegex(project, regex);
    }

    public SQLBlacklistItem getItemBySql(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String sql = sqlBlacklistItemRequest.getSql();
        if (null == sql) {
            return null;
        }
        return getSQLBlacklistManager().getSqlBlacklistItemBySql(project, sql);
    }

    @Transaction(project = 0)
    public SQLBlacklist addSqlBlacklistItem(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest)
            throws IOException {
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.updateRandomUuid();
        sqlBlacklistItem.setRegex(sqlBlacklistItemRequest.getRegex());
        sqlBlacklistItem.setSql(sqlBlacklistItemRequest.getSql());
        sqlBlacklistItem.setConcurrentLimit(sqlBlacklistItemRequest.getConcurrentLimit());
        return getSQLBlacklistManager().addSqlBlacklistItem(project, sqlBlacklistItem);
    }

    public SQLBlacklistItem checkConflictRegex(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String itemId = sqlBlacklistItemRequest.getId();
        String regex = sqlBlacklistItemRequest.getRegex();
        SQLBlacklist sqlBlacklist = getSQLBlacklistManager().getSqlBlacklist(project);
        if (null == regex || null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(itemId);
        SQLBlacklistItem regexItem = sqlBlacklist.getSqlBlacklistItemByRegex(regex);
        if (null != regexItem && !regexItem.getId().equals(originItem.getId())) {
            return regexItem;
        }
        return null;
    }

    public SQLBlacklistItem checkConflictSql(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String itemId = sqlBlacklistItemRequest.getId();
        String sql = sqlBlacklistItemRequest.getSql();
        SQLBlacklist sqlBlacklist = getSQLBlacklistManager().getSqlBlacklist(project);
        if (null == sql || null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(itemId);
        SQLBlacklistItem sqlItem = sqlBlacklist.getSqlBlacklistItemBySql(sql);
        if (null != sqlItem && !sqlItem.getId().equals(originItem.getId())) {
            return sqlItem;
        }
        return null;
    }

    @Transaction(project = 0)
    public SQLBlacklist updateSqlBlacklistItem(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest)
            throws IOException {
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setId(sqlBlacklistItemRequest.getId());
        sqlBlacklistItem.setRegex(sqlBlacklistItemRequest.getRegex());
        sqlBlacklistItem.setSql(sqlBlacklistItemRequest.getSql());
        sqlBlacklistItem.setConcurrentLimit(sqlBlacklistItemRequest.getConcurrentLimit());
        return getSQLBlacklistManager().updateSqlBlacklistItem(project, sqlBlacklistItem);
    }

    @Transaction(project = 0)
    public SQLBlacklist deleteSqlBlacklistItem(String project, String id) throws IOException {
        return getSQLBlacklistManager().deleteSqlBlacklistItem(project, id);
    }

    @Transaction(project = 0)
    public SQLBlacklist clearSqlBlacklist(String project) throws IOException {
        return getSQLBlacklistManager().clearBlacklist(project);
    }
}
