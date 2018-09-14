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

package io.kyligence.kap.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("serial")
public class QueryHistory extends RootPersistentEntity implements Comparable<QueryHistory> {
    public static final String ADJ_SLOW = "Slow";
    public static final String ADJ_PUSHDOWN = "Pushdown";
    public static final String QUERY_HISTORY_UNACCELERATED = "NEW";
    public static final String QUERY_HISTORY_ACCELERATED = "FULLY_ACCELERATED";

    @JsonProperty("sql")
    private String sql;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("latency")
    //todo: change to long
    private long latency;
    @JsonProperty("realization")
    private List<String> realization;
    @JsonProperty("query_node")
    private String queryNode;
    @JsonProperty("thread")
    private String thread;
    @JsonProperty("user")
    private String user;
    @JsonProperty("query_status")
    private QueryHistoryStatusEnum queryStatus;
    @JsonProperty("favorite")
    private String favorite;
    @JsonProperty("accelerate_status")
    private String accelerateStatus;

    //query details
    @JsonProperty("query_id")
    private String queryId;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("content")
    private List<String> content;
    @JsonProperty("total_scan_count")
    private long totalScanCount;
    @JsonProperty("total_scan_bytes")
    private long totalScanBytes;
    @JsonProperty("result_row_count")
    private long resultRowCount;
    @JsonProperty("cube_hit")
    private boolean isCubeHit = false;

    public QueryHistory(String queryId, String sql, long startTime, long latency, String queryNode, String thread,
                        String user) {
        this.queryId = queryId;
        this.updateRandomUuid();
        this.sql = sql;
        this.startTime = startTime;
        this.latency = latency;
        this.queryNode = queryNode;
        this.thread = thread;
        this.user = user;
    }

    public QueryHistory() {
        updateRandomUuid();
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public List<String> getRealization() {
        return realization;
    }

    public void setRealization(List<String> realization) {
        this.realization = realization;
    }

    public String getQueryNode() {
        return queryNode;
    }

    public void setQueryNode(String queryNode) {
        this.queryNode = queryNode;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public QueryHistoryStatusEnum getQueryStatus() {
        return queryStatus;
    }

    public void setQueryStatus(QueryHistoryStatusEnum queryStatus) {
        this.queryStatus = queryStatus;
    }

    public String getFavorite() {
        return favorite;
    }

    public void setFavorite(String favorite) {
        this.favorite = favorite;
    }

    public boolean isFavorite() {
        return this.favorite != null;
    }

    public String getAccelerateStatus() {
        return accelerateStatus;
    }

    public void setAccelerateStatus(String accelerateStatus) {
        this.accelerateStatus = accelerateStatus;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public List<String> getContent() {
        return content;
    }

    public void setContent(List<String> content) {
        this.content = content;
    }

    public long getTotalScanCount() {
        return totalScanCount;
    }

    public void setTotalScanCount(long totalScanCount) {
        this.totalScanCount = totalScanCount;
    }

    public long getTotalScanBytes() {
        return totalScanBytes;
    }

    public void setTotalScanBytes(long totalScanBytes) {
        this.totalScanBytes = totalScanBytes;
    }

    public long getResultRowCount() {
        return resultRowCount;
    }

    public void setResultRowCount(long resultRowCount) {
        this.resultRowCount = resultRowCount;
    }

    public boolean isCubeHit() {
        return isCubeHit;
    }

    public void setCubeHit(boolean cubeHit) {
        isCubeHit = cubeHit;
    }

    @Override
    public int compareTo(QueryHistory obj) {
        int comp = Long.compare(this.startTime, obj.startTime);
        if (comp != 0)
            return comp;
        else
            return this.sql.compareTo(obj.sql);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        QueryHistory entry = (QueryHistory) o;

        if (startTime != entry.startTime)
            return false;

        if (!sql.equals(entry.sql))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, startTime);
    }

    @Override
    public String toString() {
        return "QueryHistory [ realization =" + realization + ", query node =" + queryNode + ", startTime="
                + DateFormat.formatToTimeStr(startTime) + " ]";
    }

    private Field getFieldByFieldName(String fieldName) throws NoSuchFieldException {
        for (Class<?> superClass = this.getClass(); superClass != Object.class; superClass = superClass
                .getSuperclass()) {
            return superClass.getDeclaredField(fieldName);
        }
        return null;
    }

    public Object getValueByFieldName(String fieldName)
            throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field field = getFieldByFieldName(fieldName);
        Object value = null;
        if (field != null) {
            if (field.isAccessible()) {
                value = field.get(this);
            } else {
                field.setAccessible(true);
                value = field.get(this);
                field.setAccessible(false);
            }
        }
        return value;
    }
}
