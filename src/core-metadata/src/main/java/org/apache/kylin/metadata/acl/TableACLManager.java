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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.acl;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;

/**
 */
public class TableACLManager {

    private static final Logger logger = LoggerFactory.getLogger(TableACLManager.class);

    public static TableACLManager getInstance(KylinConfig config) {
        return config.getManager(TableACLManager.class);
    }

    // called by reflection
    static TableACLManager newInstance(KylinConfig config) throws IOException {
        return new TableACLManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private CachedCrudAssist<TableACL> crud;

    public TableACLManager(KylinConfig config) throws IOException {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing TableACLManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.crud = new CachedCrudAssist<TableACL>(getStore(), "/table_acl", TableACL.class) {
            @Override
            protected TableACL initEntityAfterReload(TableACL acl, String resourceName) {
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

    public TableACL getTableACLByCache(String project) {
        TableACL tableACL = crud.get(project);
        return tableACL;
    }

    public void addTableACL(String project, String name, String table, String type) throws IOException {
        TableACL tableACL = loadTableACL(project).add(name, table, type);
        crud.save(tableACL);
    }

    public void deleteTableACL(String project, String name, String table, String type) throws IOException {
        TableACL tableACL = loadTableACL(project).delete(name, table, type);
        crud.save(tableACL);
    }

    public void deleteTableACL(String project, String name, String type) throws IOException {
        TableACL tableACL = loadTableACL(project).delete(name, type);
        crud.save(tableACL);
    }

    public void deleteTableACLByTbl(String project, String table) throws IOException {
        TableACL tableACL = loadTableACL(project).deleteByTbl(table);
        crud.save(tableACL);
    }

    private TableACL loadTableACL(String project) throws IOException {
        TableACL acl = crud.get(project);
        if (acl == null) {
            acl = newTableACL(project);
        }
        return acl;
    }

    private TableACL newTableACL(String project) {
        TableACL acl = new TableACL();
        acl.init(project);
        return acl;
    }

}
