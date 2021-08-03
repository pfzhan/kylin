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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_EXPORT_TO_SECOND_STORAGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * The mechanism we used is unique for ClickHouse That‘s why we name it {@link ClickHouseLoad} instead of NJDBCLoad.
 *
 *
 */
@Slf4j
public class ClickHouseLoad extends AbstractExecutable {

    private static <T> T wrapWithExecuteException(final Callable<T> lambda) throws ExecuteException {
        try {
            return lambda.call();
        } catch (ExecuteException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecuteException(e);
        }
    }

    public static final String SOURCE_URL = "source_url";
    public static final String ROOT_PATH = "root_path";

    private static String indexPath(String workingDir, String dataflowID, String segmentID, long layoutID) {
        return String.format(Locale.ROOT, "%s%s/%s/%d", workingDir, dataflowID, segmentID, layoutID);
    }

    private List<List<LoadInfo>> loadInfos = null;

    public List<List<LoadInfo>> getLoadInfos() {
        return loadInfos;
    }

    public ClickHouseLoad() {
        this.setName(STEP_EXPORT_TO_SECOND_STORAGE);
    }

    public ClickHouseLoad(String name) {
        this.setName(name);
    }

    private String getUtParam(String key) {
        return System.getProperty(key, null);
    }

    private Engine createTableEngine() {
        val sourceType = getTableSourceType();
        return new Engine(sourceType.getTableEngineType(), getUtParam(ROOT_PATH), getUtParam(SOURCE_URL), sourceType);
    }

    private boolean isAzurePlatform() {
        return KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().startsWith("wasb");
    }

    private boolean isAwsPlatform() {
        return KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().startsWith("s3");
    }

    private boolean isUt() {
        return KylinConfig.getInstanceFromEnv().isUTEnv();
    }

    private TableSourceType getTableSourceType() {
        TableSourceType sourceType = TableSourceType.HDFS;
        if (isAwsPlatform()) {
            sourceType = TableSourceType.S3;
        } else if (isAzurePlatform()) {
            sourceType = TableSourceType.BLOB;
        } else if (isUt()) {
            sourceType = TableSourceType.UT;
        }
        return sourceType;
    }

    protected String[] selectInstances(String[] nodeNames, int shardNumber) {
        if (nodeNames.length == shardNumber)
            return nodeNames;
        throw new UnsupportedOperationException();
    }

    protected FileProvider getFileProvider(NDataflow df, String segmentId, long layoutId) {
        final String workingDir = KapConfig.wrap(df.getConfig()).getReadParquetStoragePath(df.getProject());
        String segmentLayoutRoot = indexPath(workingDir, df.getId(), segmentId, layoutId);
        return new SegmentFileProvider(segmentLayoutRoot);
    }

    private List<LoadInfo> distributeLoad(NDataflow df, IndexPlan indexPlan, TablePlan tablePlan, String[] nodeNames) {
        int ckInstances = nodeNames.length;
        return getSegmentIds() // Equivalent to scala `for comprehension`
                .stream().flatMap(segId -> getLayoutIds().stream().map(indexPlan::getLayoutEntity)
                        .filter(SecondStorageUtil::isBaseTableIndex).map(layoutEntity -> {
                            TableEntity tableEntity = tablePlan.getEntity(layoutEntity).orElse(null);
                            Preconditions.checkArgument(tableEntity != null);
                            int shardNumber = Math.min(ckInstances, tableEntity.getShardNumbers());
                            return LoadInfo.distribute(selectInstances(nodeNames, shardNumber), df.getModel(),
                                    df.getSegment(segId), getFileProvider(df, segId, layoutEntity.getId()),
                                    layoutEntity);
                        }))
                .collect(Collectors.toList());
    }

    public static class MethodContext {
        private final String project;
        private final String dataflowId;
        private final KylinConfig config;
        private final NDataflow df;
        private final String database;
        private final Function<LayoutEntity, String> prefixTableName;

        MethodContext(ClickHouseLoad load) {
            this.project = load.getProject();
            this.dataflowId = load.getParam(NBatchConstants.P_DATAFLOW_ID);
            this.config = KylinConfig.getInstanceFromEnv();
            final NDataflowManager dfMgr = NDataflowManager.getInstance(config, load.getProject());
            this.df = dfMgr.getDataflow(dataflowId);
            this.database = NameUtil.getDatabase(df);
            this.prefixTableName = s -> NameUtil.getTable(df, s.getId());
        }

        IndexPlan indexPlan() {
            return df.getIndexPlan();
        }

        TablePlan tablePlan() {
            return SecondStorage.tablePlanManager(config, project).get(dataflowId)
                    .orElseThrow(() -> new IllegalStateException(" no table plan found"));
        }

        TableFlow tableFlow() {
            return SecondStorage.tableFlowManager(config, project).get(dataflowId)
                    .orElseThrow(() -> new IllegalStateException(" no table flow found"));
        }

        boolean isIncremental(Set<String> segIds) {
            return this.df.getSegments().stream().filter(segment -> segIds.contains(segment.getId()))
                    .noneMatch(segment -> segment.getSegRange().isInfinite());
        }

        public String getProject() {
            return project;
        }

        public String getDataflowId() {
            return dataflowId;
        }

        public KylinConfig getConfig() {
            return config;
        }

        public NDataflow getDf() {
            return df;
        }

        public String getDatabase() {
            return database;
        }

        public Function<LayoutEntity, String> getPrefixTableName() {
            return prefixTableName;
        }
    }

    /**
     * update load info before use it。 For example, refresh job will set old segment id to load info.
     * @param infoList
     * @return new loadInfo list
     */
    protected List<LoadInfo> preprocessLoadInfo(List<LoadInfo> infoList) {
        return infoList;
    }

    /**
     * extend point for subclass
     */
    protected void init() {
        // do nothing
    }

    protected void preCheck() {
        SecondStorageUtil.isModelEnable(getProject(), getTargetModelId());
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        init();
        preCheck();
        final MethodContext mc = new MethodContext(this);
        return wrapWithExecuteException(() -> {
            val isIncrementalBuild = mc.isIncremental(getSegmentIds());
            Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(mc.config, mc.project);
            val tableFlowManager = SecondStorage.tableFlowManager(mc.config, mc.project);
            val partitions = tableFlowManager.listAll().stream()
                    .flatMap(tableFlow -> tableFlow.getTableDataList().stream())
                    .flatMap(tableData -> tableData.getPartitions().stream()).collect(Collectors.toList());
            Map<String, Long> nodeSizeMap = new HashMap<>();
            partitions.forEach(partition -> partition.getNodeFileMap().forEach((node, files) -> {
                Long size = nodeSizeMap.computeIfAbsent(node, n -> 0L);
                size = size + files.stream().map(SegmentFileStatus::getLen).reduce(Long::sum).orElse(0L);
                nodeSizeMap.put(node, size);
            }));
            List<NodeGroup> allGroup = nodeGroupManager.listAll();
            String[][] nodeGroups = new String[allGroup.size()][];
            ListIterator<NodeGroup> it = allGroup.listIterator();
            while (it.hasNext()) {
                nodeGroups[it.nextIndex()] = it.next().getNodeNames().toArray(new String[0]);
            }
            ShardOptions options = new ShardOptions(ShardOptions.buildReplicaSharding(nodeGroups));
            boolean isIncremental = mc.isIncremental(getSegmentIds());
            DataLoader dataLoader = new DataLoader(mc.getDatabase(), mc.getPrefixTableName(), createTableEngine(), isIncremental);
            val replicaNum = options.replicaShards().length;
            List<List<LoadInfo>> tempLoadInfos = new ArrayList<>();
            for (val shards : options.replicaShards()) {
                val sortedShards = Arrays.stream(shards)
                        .sorted(Comparator.comparingLong(node -> nodeSizeMap.getOrDefault(node, 0L)))
                        .collect(Collectors.toList());
                List<LoadInfo> infoList = distributeLoad(mc.df, mc.indexPlan(), mc.tablePlan(),
                        sortedShards.toArray(new String[] {}));
                infoList = preprocessLoadInfo(infoList);
                tempLoadInfos.add(infoList);
            }
            this.loadInfos = IntStream.range(0, tempLoadInfos.get(0).size()).mapToObj(idx -> {
                List<LoadInfo> loadInfoBatch = new ArrayList<>(replicaNum);
                for (val item : tempLoadInfos) {
                    loadInfoBatch.add(item.get(idx));
                }
                return loadInfoBatch;
            }).collect(Collectors.toList());

            for (List<LoadInfo> infoBatch : loadInfos)
                dataLoader.load(infoBatch);

            updateMeta();
            return ExecuteResult.createSucceed();
        });
    }

    protected void updateMeta() {
        Preconditions.checkArgument(this.getLoadInfos() != null, "no load info found");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final MethodContext mc = new MethodContext(this);
            val isIncrementalBuild = mc.isIncremental(getSegmentIds());
            return mc.tableFlow()
                    .update(copied -> this.getLoadInfos().stream().flatMap(Collection::stream)
                            .forEach(loadInfo -> loadInfo.upsertTableData(copied, mc.database,
                                    mc.prefixTableName.apply(loadInfo.getLayout()),
                                    isIncrementalBuild ? PartitionType.INCREMENTAL : PartitionType.FULL)));
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }
}
