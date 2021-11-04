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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLBlacklistManager {

    private static final Logger logger = LoggerFactory.getLogger(SQLBlacklistManager.class);

    private KylinConfig config;

    private CachedCrudAssist<SQLBlacklist> crud;

    public SQLBlacklistManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction()) {
            logger.info("Initializing SQLBlacklistManager with KylinConfig Id: {}", System.identityHashCode(config));
        }
        logger.info("Initializing SQLBlacklistManager with config {}", config);
        this.config = config;
        this.crud = new CachedCrudAssist<SQLBlacklist>(getStore(), SQLBlacklist.SQL_BLACKLIST_RESOURCE_ROOT, SQLBlacklist.class) {
            @Override
            public SQLBlacklist initEntityAfterReload(SQLBlacklist sqlBlacklist, String resourceName) {
                return sqlBlacklist;
            }
        };

        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public static SQLBlacklistManager getInstance(KylinConfig config) {
        return config.getManager(SQLBlacklistManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static SQLBlacklistManager newInstance(KylinConfig config) {
        try {
            String cls = SQLBlacklistManager.class.getName();
            Class<? extends SQLBlacklistManager> clz = ClassUtil.forName(cls, SQLBlacklistManager.class);
            return clz.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init SQLBlacklistManager from " + config, e);
        }
    }

    public SQLBlacklistItem getSqlBlacklistItemById(String project, String id) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItem(id);
    }

    public SQLBlacklistItem getSqlBlacklistItemByRegex(String project, String regex) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItemByRegex(regex);
    }

    public SQLBlacklistItem getSqlBlacklistItemBySql(String project, String sql) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItemBySql(sql);
    }

    public SQLBlacklist getSqlBlacklist(String project) {
        return crud.get(project);
    }

    public SQLBlacklist saveSqlBlacklist(SQLBlacklist sqlBlacklist) throws IOException {
        SQLBlacklist savedSqlBlacklist = getSqlBlacklist(sqlBlacklist.getProject());
        if (null != savedSqlBlacklist) {
            savedSqlBlacklist.setBlacklistItems(sqlBlacklist.getBlacklistItems());
            crud.save(savedSqlBlacklist);
        } else {
            crud.save(sqlBlacklist);
        }
        return sqlBlacklist;
    }

    public SQLBlacklist addSqlBlacklistItem(String project, SQLBlacklistItem sqlBlacklistItem) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            sqlBlacklist = new SQLBlacklist();
            sqlBlacklist.setProject(project);
        }
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist updateSqlBlacklistItem(String project, SQLBlacklistItem sqlBlacklistItem) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(sqlBlacklistItem.getId());
        if (null == originItem) {
            return sqlBlacklist;
        }
        originItem.setRegex(sqlBlacklistItem.getRegex());
        originItem.setSql(sqlBlacklistItem.getSql());
        originItem.setConcurrentLimit(sqlBlacklistItem.getConcurrentLimit());
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist deleteSqlBlacklistItem(String project, String id) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        sqlBlacklist.deleteSqlBlacklistItem(id);
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist clearBlacklist(String project) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        sqlBlacklist.setBlacklistItems(Lists.newArrayList());
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklistItem matchSqlBlacklist(String project, String sql) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.match(sql);
    }
}
