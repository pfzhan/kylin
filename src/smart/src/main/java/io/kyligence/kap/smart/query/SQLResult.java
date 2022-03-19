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

package io.kyligence.kap.smart.query;

import java.io.Serializable;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class SQLResult implements Serializable {

    public static final String NON_SELECT_CLAUSE = "Statement is not a select clause";

    @JsonProperty("status")
    private Status status;

    @JsonProperty("message")
    private String message;

    @JsonIgnore
    private Throwable exception;

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("project")
    private String project;

    public void writeNonQueryException(String project, String sql, long duration) {
        write(project, sql, duration, NON_SELECT_CLAUSE);
        writeExceptionInfo(NON_SELECT_CLAUSE, new SQLException(NON_SELECT_CLAUSE + ":" + sql));
    }

    public void writeExceptionInfo(String message, Throwable e) {
        this.status = Status.FAILED;
        this.message = message;
        this.exception = e;
    }

    public void writeNormalInfo(String project, String sql, long elapsed, String queryId) {
        write(project, sql, elapsed, queryId);
        if (this.exception == null) {
            this.status = Status.SUCCESS;
        }
    }

    private void write(String project, String sql, long elapsed, String queryId) {
        this.project = project;
        this.sql = sql;
        this.duration = elapsed;
        this.queryId = queryId;
    }

    public enum Status {
        SUCCESS, FAILED
    }
}