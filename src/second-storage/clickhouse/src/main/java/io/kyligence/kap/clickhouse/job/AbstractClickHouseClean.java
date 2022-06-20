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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.execution.AbstractExecutable;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractClickHouseClean extends AbstractExecutable {
    public static final String CLICKHOUSE_SHARD_CLEAN_PARAM = "P_CLICKHOUSE_SHARD_CLEAN";
    public static final String CLICKHOUSE_NODE_COUNT_PARAM = "P_CLICKHOUSE_NODE_COUNT";
    public static final String THREAD_NAME = "CLICKHOUSE_CLEAN";
    protected List<ShardCleaner> shardCleaners = new ArrayList<>();
    private int nodeCount = 10;

    public AbstractClickHouseClean() {
        super();
    }

    public AbstractClickHouseClean(Object notSetId) {
        super(notSetId);
    }

    public void setNodeCount(int nodeCount) {
        if (nodeCount > 0) {
            this.nodeCount = nodeCount;
        }
    }

    protected void saveState() {
        this.setParam(CLICKHOUSE_SHARD_CLEAN_PARAM, JsonUtil.writeValueAsStringQuietly(shardCleaners));
        this.setParam(CLICKHOUSE_NODE_COUNT_PARAM, String.valueOf(nodeCount));
    }

    protected void loadState() {
        try {
            shardCleaners = JsonUtil.readValue(this.getParam(CLICKHOUSE_SHARD_CLEAN_PARAM),
                    new TypeReference<List<ShardCleaner>>() {
                    });
            this.nodeCount = Integer.parseInt(this.getParam(CLICKHOUSE_NODE_COUNT_PARAM));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public void init() {
        internalInit();
        saveState();
    }

    protected abstract void internalInit();

    protected abstract Runnable getTask(ShardCleaner shardCleaner);

    protected void workImpl() throws ExecutionException, InterruptedException {
        val taskPool = new ThreadPoolExecutor(nodeCount, nodeCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory(THREAD_NAME));
        List<Future<?>> results = new ArrayList<>();
        shardCleaners.forEach(shardCleaner -> {
            val result = taskPool.submit(getTask(shardCleaner));
            results.add(result);
        });
        try {
            for (Future<?> result : results) {
                result.get();
            }
        } finally {
            taskPool.shutdownNow();
            closeShardClean();
        }
    }

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {
        return wrapWithExecuteException(() -> {
            loadState();
            workImpl();
            return ExecuteResult.createSucceed();
        });
    }

    protected void closeShardClean() {
        if (!shardCleaners.isEmpty()) {
            shardCleaners.forEach(shardCleaner -> shardCleaner.getClickHouse().close());
            shardCleaners.clear();
        }
    }
}
