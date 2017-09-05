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

package io.kyligence.kap.modeling.smart.query;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.modeling.smart.cube.SqlResult;
import io.kyligence.kap.query.mockup.MockupQueryExecutor;
import io.kyligence.kap.query.mockup.QueryRecord;
import io.kyligence.kap.query.mockup.Utils;

public class QueryDryRunner implements Closeable {
    final QueryStatsRecorder queryRecorder = new QueryStatsRecorder();
    private final CubeDesc cubeDesc;
    private final String[] sqls;
    private final Cache<String, QueryRecord> queryCache = CacheBuilder.newBuilder().maximumSize(20).build();
    private QueryStats queryStats;
    private ExecutorService executorService;
    private ConcurrentNavigableMap<Integer, SqlResult> queryResults = new ConcurrentSkipListMap<>();

    public QueryDryRunner(CubeDesc cubeDesc, String[] sqls) {
        this(cubeDesc, sqls, 1);
    }

    public QueryDryRunner(CubeDesc cubeDesc, String[] sqls, int threads) {
        this.cubeDesc = cubeDesc;
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
                    SqlResult result = record.getSqlResult();
                    queryResults.put(index, result);
                    if (result.getStatus() == SqlResult.Status.SUCCESS) {
                        queryRecorder.record(record);
                    }
                } finally {
                    counter.countDown();
                }
            }
        });

    }

    public void execute() throws Exception {
        if (queryStats != null && !queryResults.isEmpty()) {
            return;
        }

        final String projectName = cubeDesc.getProject();
        final File localMetaDir = prepareLocalMetaStore(projectName, cubeDesc);

        KylinConfig config = Utils.newKylinConfig(localMetaDir.getAbsolutePath());
        Utils.setLargeCuboidCombinationConf(config);

        try {
            MockupQueryExecutor queryExecutor = new MockupQueryExecutor();
            CountDownLatch latch = new CountDownLatch(sqls.length);
            for (int i = 0; i < sqls.length; i++) {
                submitQueryExecute(latch, queryExecutor, config, projectName, sqls[i], i);
            }
            latch.await();

            queryStats = queryRecorder.getResult();
        } finally {
            Utils.clearCacheForKylinConfig(config);
            FileUtils.forceDelete(localMetaDir);
        }
    }

    public QueryStats getQueryStats() {
        return queryStats;
    }

    public List<SqlResult> getQueryResults() {
        return Lists.newArrayList(queryResults.values());
    }

    private File prepareLocalMetaStore(String projName, CubeDesc cubeDesc) throws IOException, URISyntaxException {
        CubeInstance cubeInstance = CubeInstance.create(cubeDesc.getName(), cubeDesc);
        cubeInstance.setStatus(RealizationStatusEnum.READY);
        DataModelDesc modelDesc = cubeDesc.getModel();
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(projName);
        projectInstance.init();

        projectInstance.addModel(modelDesc.getName());
        projectInstance.addRealizationEntry(RealizationType.CUBE, cubeInstance.getName());

        Set<String> dumpResources = Sets.newHashSet();
        dumpResources.add(modelDesc.getResourcePath());
        for (TableRef tableRef : modelDesc.getAllTables()) {
            dumpResources.add(tableRef.getTableDesc().getResourcePath());
            projectInstance.addTable(tableRef.getTableIdentity());
        }

        String metaPath = ResourceStore.dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));
        FileUtils.writeStringToFile(new File(metaDir, cubeInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(cubeInstance), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, cubeDesc.getResourcePath()),
                JsonUtil.writeValueAsIndentString(cubeDesc), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, projectInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(projectInstance), Charset.defaultCharset());

        return metaDir;
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
