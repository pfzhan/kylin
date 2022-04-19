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
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectLoader;
import io.kyligence.kap.smart.query.SQLResult.Status;
import io.kyligence.kap.smart.query.mockup.AbstractQueryExecutor;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractQueryRunner implements Closeable {

    private final String[] sqls;
    protected KylinConfig kylinConfig;
    protected final String project;

    private final Cache<String, QueryRecord> queryCache = CacheBuilder.newBuilder().maximumSize(20).build();
    @Getter
    private final Map<String, SQLResult> queryResults = new ConcurrentSkipListMap<>();
    @Getter
    private final Map<String, Collection<OLAPContext>> olapContexts = Maps.newLinkedHashMap();

    private static final ExecutorService SUGGESTION_EXECUTOR_POOL = Executors.newFixedThreadPool(
            KylinConfig.getInstanceFromEnv().getProposingThreadNum(), new NamedThreadFactory("SuggestRunner"));

    AbstractQueryRunner(String project, String[] sqls) {
        this.project = Objects.requireNonNull(project);
        this.sqls = Objects.requireNonNull(sqls);
    }

    private void submitQueryExecute(final CountDownLatch counter, final AbstractQueryExecutor executor,
            final KylinConfig kylinConfig, final String project, final String sql) {

        SUGGESTION_EXECUTOR_POOL.execute(() -> {
            try {
                if (!isQueryCached(sql)) {
                    try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                            .setAndUnsetThreadLocalConfig(kylinConfig)) {
                        NTableMetadataManager.getInstance(autoUnset.get(), project);
                        NDataModelManager.getInstance(autoUnset.get(), project);
                        NDataflowManager.getInstance(autoUnset.get(), project);
                        NIndexPlanManager.getInstance(autoUnset.get(), project);
                        NProjectLoader.updateCache(project);
                        QueryRecord record = executor.execute(project, autoUnset.get(), sql);
                        queryCache.put(sql, record);
                    }
                }
                recordExecuteResult(sql);
            } finally {
                NProjectLoader.removeCache();
                OLAPContext.clearThreadLocalContexts();
                counter.countDown();
            }
        });
    }

    private void recordExecuteResult(String sql) {
        QueryRecord record = queryCache.getIfPresent(sql);
        if (record == null) {
            log.error("The analysis result missing for sql: {}", sql);
        } else {
            queryResults.put(sql, record.getSqlResult());
            olapContexts.get(sql).addAll(record.getOlapContexts());
        }
    }

    private boolean isQueryCached(String sql) {
        QueryRecord record = queryCache.getIfPresent(sql);
        return record != null && record.getSqlResult() != null && record.getSqlResult().getStatus() == Status.SUCCESS;
    }

    public void execute() throws IOException, InterruptedException {
        log.info("Mock query to generate OLAPContexts applied to auto-modeling.");
        KylinConfig config = prepareConfig();
        try {
            AbstractQueryExecutor queryExecutor = new MockupQueryExecutor();
            CountDownLatch latch = new CountDownLatch(sqls.length);
            for (String sql : sqls) {
                olapContexts.put(sql, Lists.newArrayList());
                submitQueryExecute(latch, queryExecutor, config, project, sql);
            }
            latch.await();
        } finally {
            cleanupConfig(config);
        }
    }

    public abstract KylinConfig prepareConfig() throws IOException;

    public abstract void cleanupConfig(KylinConfig config) throws IOException;

    public Map<String, List<OLAPContext>> filterModelViewOLAPContexts() {
        List<OLAPContext> modeViewOlapContextList = Lists.newArrayList();
        olapContexts.forEach((sql, olapContextList) -> {
            List<OLAPContext> modelViewOlapContexts = olapContextList.stream()
                    .filter(e -> StringUtils.isNotEmpty(e.getModelAlias())).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(modelViewOlapContexts)) {
                return;
            }
            modelViewOlapContexts.forEach(e -> e.sql = sql);
            modeViewOlapContextList.addAll(modelViewOlapContexts);
        });
        return modeViewOlapContextList.stream().collect(Collectors.groupingBy(OLAPContext::getModelAlias));
    }

    public Map<String, Collection<OLAPContext>> filterNonModelViewOlapContexts() {
        return olapContexts.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey, v -> v.getValue().stream()
                                .filter(e -> StringUtils.isEmpty(e.getModelAlias())).collect(Collectors.toList()),
                        (k1, k2) -> k1, LinkedHashMap::new));
    }

    @Override
    public void close() {
        queryCache.invalidateAll();
        queryResults.clear();
        olapContexts.clear();
    }
}
