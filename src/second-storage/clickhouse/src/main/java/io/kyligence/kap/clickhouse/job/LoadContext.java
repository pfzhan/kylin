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

package io.kyligence.kap.clickhouse.job;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ThreadSafe
public class LoadContext {
    public static final String CLICKHOUSE_LOAD_CONTEXT = "P_CLICKHOUSE_LOAD_CONTEXT";

    private final ConcurrentMap<String, List<String>> completedSegments;
    private final ConcurrentMap<String, List<String>> completedFiles;
    private final ConcurrentMap<String, List<String>> history;
    private final ConcurrentMap<String, List<String>> historySegments;
    private final ClickHouseLoad job;

    public LoadContext(ClickHouseLoad job) {
        completedFiles = new ConcurrentHashMap<>();
        completedSegments = new ConcurrentHashMap<>();
        history = new ConcurrentHashMap<>();
        historySegments = new ConcurrentHashMap<>();
        this.job = job;
    }

    public void finishSingleFile(CompletedFileKeyUtil keyUtil, String file) {
        completedFiles.computeIfAbsent(keyUtil.toKey(), key -> new CopyOnWriteArrayList<>()).add(file);
    }

    public void finishSegment(String segment, CompletedSegmentKeyUtil keyUtil) {
        completedSegments.computeIfAbsent(keyUtil.toKey(), key -> new CopyOnWriteArrayList<>()).add(segment);
    }

    public List<String> getHistory(CompletedFileKeyUtil keyUtil) {
        return Collections.unmodifiableList(this.history.getOrDefault(keyUtil.toKey(), Collections.emptyList()));
    }

    public List<String> getHistorySegments(CompletedSegmentKeyUtil keyUtil) {
        return Collections.unmodifiableList(this.historySegments.getOrDefault(keyUtil.toKey(), Collections.emptyList()));
    }

    @SneakyThrows
    public String serializeToString() {
        return JsonUtil.writeValueAsString(new ContextDump(completedSegments, completedFiles));
    }

    @SneakyThrows
    public static String emptyState() {
        return JsonUtil.writeValueAsString(ContextDump.getEmptyInstance());
    }

    @SneakyThrows
    public void deserializeToString(String state) {
        ContextDump historyState = JsonUtil.readValue(state, ContextDump.class);
        completedFiles.clear();
        history.clear();
        completedSegments.clear();
        historySegments.clear();

        history.putAll(historyState.getCompletedFiles() == null ? Collections.emptyMap() : historyState.getCompletedFiles());
        historySegments.putAll(historyState.getCompletedSegments() == null ? Collections.emptyMap() : historyState.getCompletedSegments());
        completedFiles.putAll(history);
        completedSegments.putAll(historySegments);
    }

    public ClickHouseLoad getJob() {
        return this.job;
    }

    public ExecutableState getJobStatus() {
        return SecondStorageUtil.getJobStatus(job.getProject(), job.getParentId());
    }

    public boolean isNewJob() {
        return history.isEmpty();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class ContextDump {
        private Map<String, List<String>> completedSegments; // CompletedSegmentKeyUtil.toKey / value: segment_ids
        private Map<String, List<String>> completedFiles; // kes: CompletedFileKeyUtil.toKey / value: hdfs files

        static ContextDump getEmptyInstance() {
            return new ContextDump(Collections.emptyMap(), Collections.emptyMap());
        }
    }

    @AllArgsConstructor
    public static class CompletedFileKeyUtil {
        private final String shardName;
        private final Long layoutId;

        public String toKey() {
            return this.shardName + "_" + layoutId;
        }
    }

    @AllArgsConstructor
    public static class CompletedSegmentKeyUtil {
        private final Long layoutId;

        public String toKey() {
            return String.valueOf(layoutId);
        }
    }
}
