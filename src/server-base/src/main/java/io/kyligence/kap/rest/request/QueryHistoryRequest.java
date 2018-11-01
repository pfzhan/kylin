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

package io.kyligence.kap.rest.request;

import lombok.Getter;

import java.util.List;

@Getter
public class QueryHistoryRequest {

    private String project;

    private long startTimeFrom;
    private long startTimeTo;
    private long latencyFrom;
    private long latencyTo;
    private String sql;

    List<String> realizations;
    List<String> accelerateStatuses;

    public QueryHistoryRequest() {

    }

    public QueryHistoryRequest(String project, long startTimeFrom, long startTimeTo, long latencyFrom, long latencyTo,
                               String sql, List<String> realizations, List<String> accelerateStatuses) {
        this.project = project;
        this.startTimeFrom = startTimeFrom;
        this.startTimeTo = startTimeTo;
        this.latencyFrom = latencyFrom;
        this.latencyTo = latencyTo;
        this.sql = sql;
        this.realizations = realizations;
        this.accelerateStatuses = accelerateStatuses;
    }
}
