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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
final class FullLoad implements Load {


    private final String database;
    private final Function<LayoutEntity, String> prefixTableName;
    private final Engine tableEngine;

    public FullLoad(String database,
                    Function<LayoutEntity, String> prefixTableName,
                    Engine tableEngine) {
        this.database = database;
        this.prefixTableName = prefixTableName;
        this.tableEngine = tableEngine;
    }

    @Override
    public void load(List<LoadInfo> loadInfoBatch) throws InterruptedException, ExecutionException, SQLException {
        val totalJdbcNum = loadInfoBatch.stream().mapToInt(item -> item.getNodeNames().length).sum();

        ArrayList<ShardLoad> shardLoads = new ArrayList<>(totalJdbcNum + 2);
        // After new shard is created, JDBC Connection is Ready.
        for (val loadInfo : loadInfoBatch) {
            String[] nodeNames = loadInfo.getNodeNames();
            Preconditions.checkArgument(nodeNames.length == loadInfo.getShardFiles().size());
            for (int idx = 0; idx < nodeNames.length; idx++) {
                final String nodeName = nodeNames[idx];
                final List<String> listParquet = loadInfo.getShardFiles().get(idx);
                ShardLoad.ShardLoadContext context = ShardLoad.ShardLoadContext.builder()
                        .jdbcURL(SecondStorageNodeHelper.resolve(nodeName))
                        .database(database)
                        .layout(loadInfo.getLayout())
                        .parquetFiles(listParquet)
                        .tableEngine(tableEngine)
                        .destTableName(prefixTableName.apply(loadInfo.getLayout()))
                        .build();
                loadInfo.setTargetDatabase(database);
                loadInfo.setTargetTable(prefixTableName.apply(loadInfo.getLayout()));
                shardLoads.add(new ShardLoad(context));
            }
        }
        final ExecutorService executorService =
                new ThreadPoolExecutor(totalJdbcNum, totalJdbcNum,
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(), new NamedThreadFactory("FullLoad"));
        CountDownLatch latch = new CountDownLatch(totalJdbcNum);
        ArrayList<Future<?>> futureList = Lists.newArrayList();

        for(ShardLoad shardLoad: shardLoads) {
            Future<?> future = executorService.submit(() -> {
                try (SetThreadName ignored = new SetThreadName("Shard %s", shardLoad.getClickHouse().getShardName())) {
                    shardLoad.setup();
                    shardLoad.loadDataIntoTempTable();
                    return true;
                } finally {
                    latch.countDown();
                }
            });
            futureList.add(future);
        }

        latch.await();

        try {
            for (Future<?> future : futureList) {
                future.get();
            }
            for(ShardLoad shardLoad: shardLoads) {
                shardLoad.commit();
            }
        } finally {
            for(ShardLoad shardLoad: shardLoads) {
                shardLoad.cleanUp();
            }
        }
    }

}
