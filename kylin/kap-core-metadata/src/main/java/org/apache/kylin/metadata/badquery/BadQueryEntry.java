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

package org.apache.kylin.metadata.badquery;

import java.util.Objects;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class BadQueryEntry extends RootPersistentEntity implements Comparable<BadQueryEntry> {

    public static final String ADJ_SLOW = "Slow";
    public static final String ADJ_PUSHDOWN = "Pushdown";

    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_OPTIMIZED = "OPTIMIZED";

    @JsonProperty("adj")
    private String adj;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("running_seconds")
    private float runningSec;
    @JsonProperty("server")
    private String server;
    @JsonProperty("thread")
    private String thread;
    @JsonProperty("user")
    private String user;
    @JsonProperty("status")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String status = STATUS_NEW;

    public BadQueryEntry(String sql, String adj, long startTime, float runningSec, String server, String thread,
            String user) {
        this.updateRandomUuid();
        this.adj = adj;
        this.sql = sql;
        this.startTime = startTime;
        this.runningSec = runningSec;
        this.server = server;
        this.thread = thread;
        this.user = user;
    }

    public BadQueryEntry() {

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public float getRunningSec() {
        return runningSec;
    }

    public void setRunningSec(float runningSec) {
        this.runningSec = runningSec;
    }

    public String getAdj() {
        return adj;
    }

    public void setAdj(String adj) {
        this.adj = adj;
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

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    @Override
    public int compareTo(BadQueryEntry obj) {
        int comp = Long.compare(this.startTime, obj.startTime);
        if (comp != 0)
            return comp;
        else
            return this.sql.compareTo(obj.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, startTime);
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BadQueryEntry entry = (BadQueryEntry) o;

        if (startTime != entry.startTime)
            return false;

        if (!sql.equals(entry.sql))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "BadQueryEntry [ adj=" + adj + ", server=" + server + ", startTime="
                + DateFormat.formatToTimeStr(startTime) + " ]";
    }
}
