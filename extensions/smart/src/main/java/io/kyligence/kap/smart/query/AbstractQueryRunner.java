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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import net.sf.ehcache.util.concurrent.ConcurrentHashMap;

public abstract class AbstractQueryRunner implements Closeable {

    protected String projectName;
    private final String[] sqls;

    private final Cache<String, QueryRecord> queryCache = CacheBuilder.newBuilder().maximumSize(20).build();
    private Map<String, SQLResult> queryResults = new ConcurrentSkipListMap<>();
    private QueryStats queryStats;
    private Map<String, Collection<OLAPContext>> olapContexts = new ConcurrentHashMap<>();

    private ExecutorService executorService;
    private final QueryStatsRecorder queryRecorder = new QueryStatsRecorder();

    public AbstractQueryRunner(String projectName, String[] sqls, int threads) {
        this.projectName = projectName;
        this.sqls = sqls;
        this.executorService = Executors.newFixedThreadPool(threads);
    }

    private void submitQueryExecute(final CountDownLatch counter, final MockupQueryExecutor executor,
            final KylinConfig kylinConfig, final String project, final String sql, final int index) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    QueryRecord record = queryCache.getIfPresent(sql);
                    if (record == null) {
                        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
                        record = executor.execute(project, sql);
                        queryCache.put(sql, record);
                    }
                    SQLResult result = record.getSqlResult();
                    queryResults.put(sql, result);
                    if (result.getStatus() == SQLResult.Status.SUCCESS) {
                        queryRecorder.record(record);
                    }
                    if (record.getOLAPContexts() != null) {
                        olapContexts.put(sql, record.getOLAPContexts());
                    }
                } finally {
                    counter.countDown();
                    KylinConfig.removeKylinConfigThreadLocal();
                }
            }
        });

    }

    public void execute() throws Exception {
        if (queryStats != null && !queryResults.isEmpty()) {
            return;
        }

        KylinConfig config = prepareConfig();
        try {
            MockupQueryExecutor queryExecutor = new MockupQueryExecutor();
            CountDownLatch latch = new CountDownLatch(sqls.length);
            for (int i = 0; i < sqls.length; i++) {
                submitQueryExecute(latch, queryExecutor, config, projectName, sqls[i], i);
            }
            latch.await();

            queryStats = queryRecorder.getResult();
        } finally {
            cleanupConfig(config);
        }
    }

    public abstract KylinConfig prepareConfig() throws Exception;

    public abstract void cleanupConfig(KylinConfig config) throws Exception;

    public QueryStats getQueryStats() {
        return queryStats;
    }

    public Map<String, SQLResult> getQueryResults() {
        return queryResults;
    }

    public Collection<OLAPContext> getOLAPContexts(String sql) {
        return olapContexts.get(sql);
    }

    public Map<String, Collection<OLAPContext>> getAllOLAPContexts() {
        return olapContexts;
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        try {
            executorService.awaitTermination(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to interrupt.", e);
        }
    }
}
