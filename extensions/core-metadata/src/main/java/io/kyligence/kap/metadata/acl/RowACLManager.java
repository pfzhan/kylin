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

package io.kyligence.kap.metadata.acl;

import static io.kyligence.kap.metadata.acl.RowACL.concatConds;
import static io.kyligence.kap.metadata.acl.RowACL.getColumnWithType;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RowACLManager {

    private static final Logger logger = LoggerFactory.getLogger(RowACLManager.class);

    public static RowACLManager getInstance(KylinConfig config) {
        return config.getManager(RowACLManager.class);
    }

    // called by reflection
    static RowACLManager newInstance(KylinConfig config) throws IOException {
        return new RowACLManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> RowACL
    private CaseInsensitiveStringCache<RowACL> rowACLMap;
    private CachedCrudAssist<RowACL> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    public RowACLManager(KylinConfig config) throws IOException {
        logger.info("Initializing RowACLManager with config " + config);
        this.config = config;
        this.rowACLMap = new CaseInsensitiveStringCache<>(config, "row_acl");
        this.crud = new CachedCrudAssist<RowACL>(getStore(), "/row_acl", "", RowACL.class, rowACLMap) {
            @Override
            protected RowACL initEntityAfterReload(RowACL acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };

        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new RowACLSyncListener(), "row_acl");
    }

    private class RowACLSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                if (event == Event.DROP)
                    rowACLMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
            broadcaster.notifyProjectACLUpdate(cacheKey);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public RowACL getRowACLByCache(String project) {
        try (AutoLock l = lock.lockForRead()) {
            RowACL rowACL = rowACLMap.get(project);
            if (rowACL == null) {
                return newRowACL(project);
            }
            return rowACL;
        }
    }

    public void addRowACL(String project, String name, String table, RowACL.ColumnToConds condsWithColumn, String type)
            throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            RowACL rowACL = loadRowACL(project).add(name, table, condsWithColumn, type);
            crud.save(rowACL);
        }
    }

    public void updateRowACL(String project, String name, String table, RowACL.ColumnToConds condsWithColumn,
            String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            RowACL rowACL = loadRowACL(project).update(name, table, condsWithColumn, type);
            crud.save(rowACL);
        }
    }

    public void deleteRowACL(String project, String name, String table, String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            RowACL rowACL = loadRowACL(project).delete(name, table, type);
            crud.save(rowACL);
        }
    }

    public void deleteRowACL(String project, String name, String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            RowACL rowACL = loadRowACL(project).delete(name, type);
            crud.save(rowACL);
        }
    }

    public void deleteRowACLByTbl(String project, String table) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            RowACL rowACL = loadRowACL(project).deleteByTbl(table);
            crud.save(rowACL);
        }
    }

    private RowACL loadRowACL(String project) throws IOException {
        RowACL acl = crud.reload(project);
        if (acl == null) {
            acl = newRowACL(project);
        }
        return acl;
    }

    private RowACL newRowACL(String project) {
        RowACL acl = new RowACL();
        acl.updateRandomUuid();
        acl.init(project);
        return acl;
    }

    // ============================================================================

    public String preview(String project, String table, RowACL.ColumnToConds condsWithColumn) throws IOException {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return concatConds(condsWithColumn, columnWithType);
    }

    public Map<String, String> getQueryUsedTblToConds(String project, String name, String type) {
        return getRowACLByCache(project).getQueryUsedTblToConds(project, name, type);
    }

}
