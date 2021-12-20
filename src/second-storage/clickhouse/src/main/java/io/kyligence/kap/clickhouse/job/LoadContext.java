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

import com.fasterxml.jackson.core.type.TypeReference;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.ExecutableState;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@ThreadSafe
public class LoadContext {
    public static final String CLICKHOUSE_LOAD_CONTEXT = "P_CLICKHOUSE_LOAD_CONTEXT";

    private final List<String> completedSegments;
    private final List<String> completedFiles;
    private final List<String> history;
    private final List<String> historySegments;
    private final ClickHouseLoad job;

    public LoadContext(ClickHouseLoad job) {
        completedFiles = new CopyOnWriteArrayList<>();
        completedSegments = new CopyOnWriteArrayList<>();
        history = new ArrayList<>();
        historySegments = new ArrayList<>();
        this.job = job;
    }

    public void finishSingleFile(String file) {
        completedFiles.add(file);
    }

    public void finishSegment(String segment) {
        completedSegments.add(segment);
    }

    public List<String> getHistory() {
        return Collections.unmodifiableList(this.history);
    }

    public List<String> getHistorySegments() {
        return Collections.unmodifiableList(this.historySegments);
    }

    @SneakyThrows
    public String serializeToString() {
        Map<String, List<String>> state = new HashMap<>();
        state.put("completedSegments", completedSegments);
        state.put("completedFiles", completedFiles);
        return JsonUtil.writeValueAsString(state);
    }

    public static String emptyState() {
        return "{\"completedSegments\":[],\"completedFiles\":[]}";
    }

    @SneakyThrows
    public void deserializeToString(String state) {
        Map<String, List<String>> historyState = JsonUtil.readValue(state, new TypeReference<Map<String, List<String>>>() {
        });
        completedFiles.clear();
        history.clear();
        completedSegments.clear();
        historySegments.clear();

        history.addAll(historyState.getOrDefault("completedFiles", Collections.emptyList()));
        historySegments.addAll(historyState.getOrDefault("completedSegments", Collections.emptyList()));
        completedFiles.addAll(history);
        completedSegments.addAll(historySegments);
    }

    public ClickHouseLoad getJob() {
        return this.job;
    }

    public ExecutableState getJobStatus() {
        return SecondStorageUtil.getJobStatus(job.getProject(), job.getParentId());
    }

    public boolean isNewJob() {
        return CollectionUtils.isEmpty(history);
    }
}
