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

package org.apache.kylin.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class QueryTrace {

    // span name
    public static final String HTTP_RECEPTION = "HTTP_RECEPTION";
    public static final String GET_ACL_INFO = "GET_ACL_INFO";
    public static final String SQL_TRANSFORMATION = "SQL_TRANSFORMATION";
    public static final String SQL_PARSE_AND_OPTIMIZE = "SQL_PARSE_AND_OPTIMIZE";
    public static final String MODEL_MATCHING = "MODEL_MATCHING";
    public static final String PREPARE_AND_SUBMIT_JOB = "PREPARE_AND_SUBMIT_JOB";
    public static final String WAIT_FOR_EXECUTION = "WAIT_FOR_EXECUTION";
    public static final String EXECUTION = "EXECUTION";
    public static final String FETCH_RESULT = "FETCH_RESULT";
    /**
     * SPARK_JOB_EXECUTION: PREPARE_AND_SUBMIT_JOB + WAIT_FOR_EXECUTION + EXECUTION + FETCH_RESULT
     */
    public static final String SPARK_JOB_EXECUTION = "SPARK_JOB_EXECUTION";

    public static final String SQL_PUSHDOWN_TRANSFORMATION = "SQL_PUSHDOWN_TRANSFORMATION";
    public static final String HIT_CACHE = "HIT_CACHE";

    // group name
    static final String PREPARATION = "PREPARATION";
    static final String JOB_EXECUTION = "JOB_EXECUTION";

    public static final Map<String, String> SPAN_GROUPS = new HashMap<>();
    static {
        SPAN_GROUPS.put(GET_ACL_INFO, PREPARATION);
        SPAN_GROUPS.put(SQL_TRANSFORMATION, PREPARATION);
        SPAN_GROUPS.put(SQL_PARSE_AND_OPTIMIZE, PREPARATION);
        SPAN_GROUPS.put(MODEL_MATCHING, PREPARATION);

        SPAN_GROUPS.put(PREPARE_AND_SUBMIT_JOB, JOB_EXECUTION);
        SPAN_GROUPS.put(WAIT_FOR_EXECUTION, JOB_EXECUTION);
        SPAN_GROUPS.put(EXECUTION, JOB_EXECUTION);
        SPAN_GROUPS.put(FETCH_RESULT, JOB_EXECUTION);
    }

    @Getter
    private List<Span> spans = new LinkedList<>();

    public Optional<Span> getLastSpan() {
        return spans.isEmpty() ? Optional.empty() : Optional.of(spans.get(spans.size() - 1));
    }

    public void endLastSpan() {
        getLastSpan().ifPresent(span -> {
            if (span.duration == -1) {
                span.duration = System.currentTimeMillis() - span.start;
            }
        });
    }

    public void startSpan(String name) {
        endLastSpan();
        spans.add(new Span(name, System.currentTimeMillis()));
    }

    public void appendSpan(String name, long duration) {
        spans.add(new Span(name,
                getLastSpan().map(span -> span.getStart() + span.getDuration()).orElse(System.currentTimeMillis()),
                duration));
    }

    public void amendLast(String name, long endAt) {
        for (int i = spans.size() - 1; i >= 0; i--) {
            if (spans.get(i).name.equals(name)) {
                spans.get(i).duration = endAt - spans.get(i).start;
                return;
            }
        }
    }

    public long calculateDuration(String name, long endAt) {
        for (int i = spans.size() - 1; i >= 0; i--) {
            if (spans.get(i).name.equals(name)) {
                long duration = endAt - spans.get(i).start;
                return duration;
            }
        }
        return -1;
    }

    public void setDuration(String name, long duration) {
        for (int i = spans.size() - 1; i >= 0; i--) {
            if (spans.get(i).name.equals(name)) {
                spans.get(i).duration = duration;
            }
        }
    }

    public void clear() {
        spans = new LinkedList<>();
    }

    public List<Span> spans() {
        return spans;
    }

    @Data
    @NoArgsConstructor
    public static class Span {
        String name;

        String group;

        long start;

        long duration = -1;

        public Span(String name, long start, long duration) {
            this.name = name;
            this.start = start;
            this.duration = duration;
            this.group = SPAN_GROUPS.get(name);
        }

        public Span(String name, long start) {
            this.name = name;
            this.start = start;
            this.group = SPAN_GROUPS.get(name);
        }
    }
}
