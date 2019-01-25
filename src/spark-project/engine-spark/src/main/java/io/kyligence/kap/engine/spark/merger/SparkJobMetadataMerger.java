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

package io.kyligence.kap.engine.spark.merger;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Set;
import java.util.TimeZone;

import com.google.common.collect.Sets;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Getter;
import lombok.val;

public abstract class SparkJobMetadataMerger extends MetadataMerger {
    private static final Logger log = LoggerFactory.getLogger(SparkJobMetadataMerger.class);
    @Getter
    private final String project;

    protected SparkJobMetadataMerger(KylinConfig config, String project) {
        super(config);
        this.project = project;
    }

    @Override
    public NDataLayout[] merge(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteStore) {
        return new NDataLayout[0];
    }

    @Override
    public void mergeAnalysis(AbstractExecutable abstractExecutable) {
        try (val remoteStore = ExecutableUtils.getRemoteStore(getConfig(), abstractExecutable)) {
            val dataflowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val remoteConfig = remoteStore.getConfig();
            final NTableMetadataManager remoteTblMgr = NTableMetadataManager.getInstance(remoteConfig, project);
            final NTableMetadataManager localTblMgr = NTableMetadataManager.getInstance(getConfig(), project);
            final NDataModel dataModel = NDataflowManager.getInstance(getConfig(), project).getDataflow(dataflowId)
                    .getModel();
            mergeAndUpdateTableExt(localTblMgr, remoteTblMgr, dataModel.getRootFactTableName());
            final Set<String> lookupMerged = Sets.newHashSet();
            for (final JoinTableDesc lookupDesc : dataModel.getJoinTables()) {
                final String lookupTable = lookupDesc.getTable();
                // The same lookup table should not be merged twice.
                if (lookupMerged.contains(lookupTable)) {
                    continue;
                }
                mergeAndUpdateTableExt(localTblMgr, remoteTblMgr, lookupDesc.getTable());
                lookupMerged.add(lookupTable);
            }
        }
    }

    private void mergeAndUpdateTableExt(NTableMetadataManager localTblMgr, NTableMetadataManager remoteTblMgr,
            String tableName) {
        val localFactTblExt = localTblMgr.getOrCreateTableExt(tableName);
        val remoteFactTblExt = remoteTblMgr.getOrCreateTableExt(tableName);

        localTblMgr.mergeAndUpdateTableExt(localFactTblExt, remoteFactTblExt);
    }

    protected void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids) {
        String model = buildTask.getTargetModel();
        long buildEndTime = buildTask.getParent().getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = 0;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            byteSize += dataCuboid.getByteSize();
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ZoneId zoneId = TimeZone.getTimeZone(kylinConfig.getTimeZone()).toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(buildEndTime).atZone(zoneId).toLocalDate();
        long startOfDay = localDate.atStartOfDay().atZone(zoneId).toInstant().toEpochMilli();
        // update
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig,
                buildTask.getProject());
        jobStatisticsManager.updateStatistics(startOfDay, model, duration, byteSize);
    }
}
