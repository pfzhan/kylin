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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractClickHouseClean extends AbstractExecutable {
    public static final String CLICKHOUSE_SHARD_CLEAN_PARAM = "P_CLICKHOUSE_SHARD_CLEAN";
    public static final String CLICKHOUSE_NODE_COUNT_PARAM = "P_CLICKHOUSE_NODE_COUNT";
    public static final String THREAD_NAME = "CLICKHOUSE_CLEAN";
    protected List<ShardClean> shardCleanList = new ArrayList<>();
    private int nodeCount = 10;

    public void setNodeCount(int nodeCount) {
        if (nodeCount > 0) {
            this.nodeCount = nodeCount;
        }
    }

    protected void saveState() {
        this.setParam(CLICKHOUSE_SHARD_CLEAN_PARAM, JsonUtil.writeValueAsStringQuietly(shardCleanList));
        this.setParam(CLICKHOUSE_NODE_COUNT_PARAM, String.valueOf(nodeCount));
    }

    protected void loadState() {
        try {
            shardCleanList = JsonUtil.readValue(this.getParam(CLICKHOUSE_SHARD_CLEAN_PARAM), new TypeReference<List<ShardClean>>() {
            });
            this.nodeCount = Integer.parseInt(this.getParam(CLICKHOUSE_NODE_COUNT_PARAM));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private static <T> T wrapWithExecuteException(final Callable<T> lambda) throws ExecuteException {
        try {
            return lambda.call();
        } catch (ExecuteException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecuteException(e);
        }
    }

    public void init() {
        internalInit();
        saveState();
    }

    protected abstract void internalInit();

    protected abstract Runnable getTask(ShardClean shardClean);

    protected void workImpl() throws ExecutionException, InterruptedException {
        val taskPool = new ThreadPoolExecutor(nodeCount, nodeCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory(THREAD_NAME));
        List<Future<?>> results = new ArrayList<>();
        shardCleanList.forEach(shardClean -> {
            val result = taskPool.submit(getTask(shardClean));
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
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        return wrapWithExecuteException(() -> {
            loadState();
            workImpl();
            return ExecuteResult.createSucceed();
        });
    }

    protected void closeShardClean() {
        if (!shardCleanList.isEmpty()) {
            shardCleanList.forEach(shardClean -> shardClean.getClickHouse().close());
            shardCleanList.clear();
        }
    }
}
