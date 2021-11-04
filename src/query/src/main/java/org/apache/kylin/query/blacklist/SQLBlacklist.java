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

package org.apache.kylin.query.blacklist;

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import lombok.Data;

@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SQLBlacklist extends RootPersistentEntity implements Serializable {

    public static final String SQL_BLACKLIST_RESOURCE_ROOT = GLOBAL_PROJECT + "/sql_blacklist";

    public SQLBlacklist() {
        updateRandomUuid();
    }

    @JsonProperty("project")
    private String project;

    @JsonProperty("blacklist_items")
    private List<SQLBlacklistItem> blacklistItems;

    public List<SQLBlacklistItem> getBlacklistItems() {
        return blacklistItems;
    }

    public void setBlacklistItems(List<SQLBlacklistItem> blacklistItems) {
        this.blacklistItems = blacklistItems;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void addBlacklistItem(SQLBlacklistItem sqlBlacklistItem) {
        if (null == this.blacklistItems) {
            this.blacklistItems = Lists.newArrayList();
        }
        this.blacklistItems.add(sqlBlacklistItem);
    }

    public SQLBlacklistItem getSqlBlacklistItem(String id) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (id.equals(sqlBlacklistItem.getId())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem getSqlBlacklistItemByRegex(String regex) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (regex.equals(sqlBlacklistItem.getRegex())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem getSqlBlacklistItemBySql(String sql) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (sql.equals(sqlBlacklistItem.getSql())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem match(String sql) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (sqlBlacklistItem.match(sql)) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public void deleteSqlBlacklistItem(String id) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return;
        }
        List<SQLBlacklistItem> newBlacklistItems = Lists.newArrayList();
        for (SQLBlacklistItem item : blacklistItems) {
            if (!item.getId().equals(id)) {
                newBlacklistItems.add(item);
            }
        }
        setBlacklistItems(newBlacklistItems);
    }

    @Override
    public String resourceName() {
        return project;
    }
}
