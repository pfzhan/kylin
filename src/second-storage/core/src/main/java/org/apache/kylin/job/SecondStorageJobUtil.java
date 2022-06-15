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
package org.apache.kylin.job;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.glassfish.jersey.internal.guava.Sets;
import org.msgpack.core.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;


public class SecondStorageJobUtil extends ExecutableUtil {

    private static boolean isValidateSegments(NDataSegment segment, SegmentRange<?> range) {
        return segment.getSegRange().startStartMatch(range)
                && segment.getSegRange().endEndMatch(range)
                && (segment.getStatus() == SegmentStatusEnum.READY
                ||
                segment.getStatus() == SegmentStatusEnum.WARNING
        );
    }

    @Override
    public void computeLayout(JobParam jobParam) {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String project = jobParam.getProject();
        final String model = jobParam.getModel();
        final NDataflow df = NDataflowManager.getInstance(config, project)
                .getDataflow(model);

        List<NDataSegment> segments = df.getSegments().stream()
                .filter(segment -> isValidateSegments(segment, segment.getSegRange()))
                .collect(Collectors.toList());

        if (segments.isEmpty()) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getAddJobCheckSegmentReadyFail());
        }

        Manager<TablePlan> tablePlanManager = SecondStorage.tablePlanManager(config, project);
        Manager<TableFlow> tableFlowManager = SecondStorage.tableFlowManager(config, project);
        TablePlan plan = tablePlanManager.makeSureRootEntity(model);
        tableFlowManager.makeSureRootEntity(model);
        Map<Long, List<LayoutEntity>> layouts = segments.stream()
                .flatMap(nDataSegment -> nDataSegment.getLayoutsMap().values().stream())
                .map(NDataLayout::getLayout)
                .filter(SecondStorageUtil::isBaseTableIndex)
                .collect(Collectors.groupingBy(LayoutEntity::getIndexId));

        NDataModel dataModel = df.getModel();
        if (!segments.get(0).getSegRange().isInfinite()) {
            LayoutEntity baseTableIndex = segments.get(0).getIndexPlan().getBaseTableLayout();
            String partitionCol = dataModel.getPartitionDesc().getPartitionDateColumn();
            Preconditions.checkState(baseTableIndex.getColumns().stream()
                            .map(TblColRef::getTableDotName).anyMatch(col -> Objects.equals(col, partitionCol)),
                    "Table index should contains partition column " + partitionCol
            );
        }

        Set<LayoutEntity> processed = Sets.newHashSet();
        for (Map.Entry<Long, List<LayoutEntity>> entry : layouts.entrySet()) {
            LayoutEntity layoutEntity =
                    entry.getValue().stream().filter(l -> l.isBaseIndex() && IndexEntity.isTableIndex(l.getId())).findFirst().orElse(null);
            if (layoutEntity != null) {
                processed.add(layoutEntity);
                plan = plan.createTableEntityIfNotExists(layoutEntity, true);
            }
        }
        jobParam.setProcessLayouts(new HashSet<>(processed));
    }
}
