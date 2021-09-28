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
package io.kyligence.kap.clickhouse;

import static io.kyligence.kap.secondstorage.factory.SecondStorageFactoryConstant.STORAGE_SEGMENT_METADATA_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_JOB_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_MODEL_CLEAN_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_NODE_CLEAN_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_SEGMENT_CLEAN_FACTORY;

import java.util.HashMap;
import java.util.Map;

import io.kyligence.kap.clickhouse.factory.ClickHouseMetadataFactory;
import io.kyligence.kap.secondstorage.factory.SecondStorageFactoryUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.ClickHouseConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageStepFactory;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.jdbc.JdbcDialects;

import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseJob;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseMerge;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseProjectCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseRefresh;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.clickhouse.metadata.ClickHouseFlowManager;
import io.kyligence.kap.clickhouse.metadata.ClickHouseManager;
import io.kyligence.kap.clickhouse.metadata.ClickHouseNodeGroupManager;
import io.kyligence.kap.secondstorage.SecondStorageConfigLoader;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStoragePlugin;
import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;

public class ClickHouseStorage implements SecondStoragePlugin {

    public ClickHouseStorage() {
        reloadNodeMap();
    }

    /**
     * clean node mapping cache
     */
    public static void reloadNodeMap() {
        ClickHouseConfigLoader.getInstance().refresh();
        Cluster cluster = ClickHouseConfigLoader.getInstance().getCluster();
        SecondStorageNodeHelper.clear();
        SecondStorageNodeHelper.initFromCluster(cluster, node -> {
            Map<String, String> param = new HashMap<>(4);
            if (StringUtils.isNotEmpty(cluster.getKeepAliveTimeout())) {
                param.put(ClickHouse.KEEP_ALIVE_TIMEOUT, cluster.getKeepAliveTimeout());
            }
            if (StringUtils.isNotEmpty(cluster.getSocketTimeout())) {
                param.put(ClickHouse.SOCKET_TIMEOUT, cluster.getSocketTimeout());
            }
            if (StringUtils.isNotEmpty(node.getUser())) {
                param.put(ClickHouse.USER, node.getUser());
            }
            if (StringUtils.isNotEmpty(node.getPassword())) {
                param.put(ClickHouse.PASSWORD, node.getPassword());
            }
            return ClickHouse.buildUrl(node.getIp(), node.getPort(), param);
        });
    }

    @Override
    public boolean ready() {
        ClickHouseConfig config = ClickHouseConfig.getInstanceFromEnv();
        return StringUtils.isNotEmpty(config.getClusterConfig());
    }

    @Override
    public String queryCatalog() {
        ClickHouseConfig config = ClickHouseConfig.getInstanceFromEnv();
        return config.getQueryCatalog();
    }

    @Override
    public Manager<TableFlow> tableFlowManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseFlowManager.class);
    }

    @Override
    public Manager<TablePlan> tablePlanManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseManager.class);
    }

    @Override
    public Manager<NodeGroup> nodeGroupManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseNodeGroupManager.class);
    }

    @Override
    public SecondStorageConfigLoader getConfigLoader() {
        return ClickHouseConfigLoader.getInstance();
    }

    static {
        JdbcDialects.registerDialect(ClickHouseDialect$.MODULE$);
        JobFactory.register(STORAGE_JOB_FACTORY, new ClickHouseJob.StorageJobFactory());
        JobFactory.register(STORAGE_MODEL_CLEAN_FACTORY, new ClickHouseModelCleanJob.ModelCleanJobFactory());
        JobFactory.register(STORAGE_NODE_CLEAN_FACTORY, new ClickHouseProjectCleanJob.ProjectCleanJobFactory());
        JobFactory.register(STORAGE_SEGMENT_CLEAN_FACTORY, new ClickHouseSegmentCleanJob.SegmentCleanJobFactory());

        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageLoadStep.class, ClickHouseLoad::new);
        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageRefreshStep.class, ClickHouseRefresh::new);
        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageMergeStep.class, ClickHouseMerge::new);

        SecondStorageFactoryUtils.register(STORAGE_SEGMENT_METADATA_FACTORY, new ClickHouseMetadataFactory());
    }
}