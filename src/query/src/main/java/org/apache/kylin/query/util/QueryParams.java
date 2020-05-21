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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryParams {

    String project;
    String sql;
    String defaultSchema;
    SQLException sqlException;
    boolean isSelect;
    boolean isPrepare;
    boolean isForced;
    QueryContext.AclInfo aclInfo;
    int limit;
    int offset;
    boolean isCCNeeded;
    KylinConfig kylinConfig;

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
    }

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare, boolean isSelect,
            boolean isForced) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
        this.isSelect = isSelect;
        this.isForced = isForced;
    }

    public QueryParams(String project, String sql, String defaultSchema, boolean isPrepare, SQLException sqlException,
            boolean isForced, boolean isSelect, int limit, int offset) {
        this.project = project;
        this.sql = sql;
        this.defaultSchema = defaultSchema;
        this.isPrepare = isPrepare;
        this.sqlException = sqlException;
        this.isForced = isForced;
        this.isSelect = isSelect;
        this.limit = limit;
        this.offset = offset;
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
}
