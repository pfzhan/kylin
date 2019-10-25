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

package org.apache.kylin.query.security;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.constant.Constant;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;

public class AclQueryInterceptor {

    private AclQueryInterceptor() {
    }

    public static void intercept(List<OLAPContext> contexts) {

        if (!KylinConfig.getInstanceFromEnv().isAclTCREnabled()) {
            return;
        }

        if (hasAdminPermission(contexts)) {
            return;
        }
        final String project = getProjectName(contexts);
        // <"DB1.TABLE1":["COLUMN1"], "DB2.TABLE2":["COLUMN2"]>
        final Map<String, Set<String>> dbTblColumns = Maps.newHashMap();
        contexts.stream().filter(Objects::nonNull).forEach(ctx -> {
            ctx.allTableScans.stream().filter(Objects::nonNull).forEach(tableScan -> {
                String dbTblName = tableScan.getTableRef().getTableIdentity();
                if (!dbTblColumns.containsKey(dbTblName)) {
                    dbTblColumns.put(dbTblName, Sets.newHashSet());
                }
            });
            ctx.allColumns.stream().filter(Objects::nonNull).forEach(tblColRef -> {
                String dbTblName = tblColRef.getTableRef().getTableIdentity();
                if (!dbTblColumns.containsKey(dbTblName)) {
                    dbTblColumns.put(dbTblName, Sets.newHashSet());
                }
                ColumnDesc columnDesc = tblColRef.getColumnDesc();
                if (columnDesc.isComputedColumn()) {
                    handleCC(project, columnDesc, dbTblColumns);
                } else {
                    dbTblColumns.get(dbTblName).add(tblColRef.getName());
                }
            });
        });

        AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project).failFastUnauthorizedTableColumn(
                getUsername(contexts), Sets.newHashSet(getGroups(contexts)), dbTblColumns).ifPresent(identifier -> {
                    throw new AccessDeniedException(identifier);
                });
    }

    private static void handleCC(String project, ColumnDesc columnDesc, final Map<String, Set<String>> dbTblColumns) {
        //<"DB1.TABLE1":["COLUMN1"]>
        ComputedColumnUtil.getCCUsedColsMapWithProject(project, columnDesc).forEach((dbTblName, columns) -> {
            if (!dbTblColumns.containsKey(dbTblName)) {
                dbTblColumns.put(dbTblName, Sets.newHashSet());
            }
            dbTblColumns.get(dbTblName).addAll(columns);
        });
    }

    protected static String getProjectName(List<OLAPContext> contexts) {
        return contexts.get(0).olapSchema.getProjectName();
    }

    protected static String getUsername(List<OLAPContext> contexts) {
        return contexts.get(0).olapAuthen.getUsername();
    }

    protected static List<String> getGroups(List<OLAPContext> contexts) {
        return contexts.get(0).olapAuthen.getRoles();
    }

    protected static boolean hasAdminPermission(List<OLAPContext> contexts) {
        if (CollectionUtils.isEmpty(contexts)) {
            return false;
        }
        return getGroups(contexts).stream().anyMatch(Constant.ROLE_ADMIN::equals) || contexts.get(0).hasAdminPermission;
    }
}
