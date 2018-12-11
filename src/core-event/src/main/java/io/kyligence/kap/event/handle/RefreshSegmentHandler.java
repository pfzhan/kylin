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
package io.kyligence.kap.event.handle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.RefreshSegmentEvent;

public class RefreshSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(RefreshSegmentHandler.class);

    @Override
    public AbstractExecutable createJob(EventContext eventContext) {
        RefreshSegmentEvent event = (RefreshSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);

        SegmentRange tobeRefreshSegmentRange = event.getSegmentRange();
        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        AbstractExecutable job;
        List<NCuboidLayout> layouts = new ArrayList<>();
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            return null;
        }

        for (Map.Entry<Long, NDataCuboid> cuboid : readySegments.getLatestReadySegment().getCuboidsMap().entrySet()) {
            layouts.add(cuboid.getValue().getCuboidLayout());
        }
        if (CollectionUtils.isEmpty(layouts)) {
            return null;
        }

        Set<NDataSegment> segments = Sets.newHashSet();
        for (NDataSegment readySegment : readySegments) {
            SegmentRange readySegmentRange = readySegment.getSegRange();
            if (tobeRefreshSegmentRange.overlaps(readySegmentRange)) {
                NDataSegment tobeRefreshSegment = dfMgr.refreshSegment(df, readySegmentRange);
                segments.add(tobeRefreshSegment);
            }
        }

        if (CollectionUtils.isEmpty(segments)) {
            return null;
        }

        job = NSparkCubingJob.create(segments, Sets.<NCuboidLayout> newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_REFRESH);

        return job;
    }

}
