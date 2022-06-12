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

package org.apache.kylin.query.util;

import java.sql.SQLException;

import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kyligence.kap.query.engine.PrepareSqlStateParam;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class QueryParams {

    String project;
    String sql;
    String defaultSchema;
    String executeAs;
    String prepareSql;
    String sparkQueue;
    boolean isSelect;
    boolean isPrepare;
    boolean forcedToIndex;
    boolean isForcedToPushDown;
    boolean isCCNeeded;
    boolean isPrepareStatementWithParams;
    boolean partialMatchIndex;
    boolean acceptPartial;
    boolean isACLDisabledOrAdmin;
    int limit;
    int offset;
    ForceToTieredStorage forcedToTieredStorage;
    SQLException sqlException;
    QueryContext.AclInfo aclInfo;
    @JsonIgnore
    KylinConfig kylinConfig;
    PrepareSqlStateParam[] params;

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
    }

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare, boolean isSelect,
            boolean isForcedToPushDown) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
        this.isSelect = isSelect;
        this.isForcedToPushDown = isForcedToPushDown;
    }

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare, SQLException sqlException,
                       boolean isForcedToPushDown) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
        this.sqlException = sqlException;
        this.isForcedToPushDown = isForcedToPushDown;
    }

    public QueryParams(KylinConfig kylinConfig, String sql, String project, int limit, int offset, String defaultSchema,
            boolean isCCNeeded) {
        this.kylinConfig = kylinConfig;
        this.sql = sql;
        this.project = project;
        this.limit = limit;
        this.offset = offset;
        this.defaultSchema = defaultSchema;
        this.isCCNeeded = isCCNeeded;
    }

    public QueryParams(KylinConfig kylinConfig, String sql, String project, int limit, int offset, boolean isCCNeeded,
                       String executeAs) {
        this.kylinConfig = kylinConfig;
        this.sql = sql;
        this.project = project;
        this.limit = limit;
        this.offset = offset;
        this.isCCNeeded = isCCNeeded;
        this.executeAs = executeAs;
    }
}
