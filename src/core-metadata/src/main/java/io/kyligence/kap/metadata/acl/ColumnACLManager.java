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

import java.io.IOException;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ColumnACLManager {

    private static final Logger logger = LoggerFactory.getLogger(ColumnACLManager.class);

    public static ColumnACLManager getInstance(KylinConfig config) {
        return config.getManager(ColumnACLManager.class);
    }

    // called by reflection
    static ColumnACLManager newInstance(KylinConfig config) {
        return new ColumnACLManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> TableACL
    private CachedCrudAssist<ColumnACL> crud;

    public ColumnACLManager(KylinConfig config) {
        logger.info("Initializing ColumnACLManager with config " + config);
        this.config = config;
        this.crud = new CachedCrudAssist<ColumnACL>(getStore(), "/column_acl", "", ColumnACL.class) {
            @Override
            protected ColumnACL initEntityAfterReload(ColumnACL acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };

        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public ColumnACL getColumnACLByCache(String project) {
        ColumnACL columnACL = crud.get(project);
        if (columnACL == null) {
            return newColumnACL(project);
        }
        return columnACL;
    }

    public void addColumnACL(String project, String name, String table, Set<String> columns, String type)
            throws IOException {
        ColumnACL columnACL = loadColumnACL(project).add(name, table, columns, type);
        crud.save(columnACL);
    }

    public void updateColumnACL(String project, String name, String table, Set<String> columns, String type)
            throws IOException {
        ColumnACL columnACL = loadColumnACL(project).update(name, table, columns, type);
        crud.save(columnACL);
    }

    public void deleteColumnACL(String project, String name, String table, String type) throws IOException {
        ColumnACL columnACL = loadColumnACL(project).delete(name, table, type);
        crud.save(columnACL);
    }

    public void deleteColumnACL(String project, String name, String type) throws IOException {
        ColumnACL columnACL = loadColumnACL(project).delete(name, type);
        crud.save(columnACL);
    }

    public void deleteColumnACLByTbl(String project, String table) throws IOException {
        ColumnACL columnACL = loadColumnACL(project).deleteByTbl(table);
        crud.save(columnACL);
    }

    private ColumnACL loadColumnACL(String project) throws IOException {
        ColumnACL acl = crud.get(project);
        if (acl == null) {
            acl = newColumnACL(project);
        }
        return acl;
    }

    private ColumnACL newColumnACL(String project) {
        ColumnACL acl = new ColumnACL();
        acl.updateRandomUuid();
        acl.init(project);
        return acl;
    }

}