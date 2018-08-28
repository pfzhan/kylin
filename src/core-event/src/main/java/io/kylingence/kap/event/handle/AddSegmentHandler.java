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
package io.kylingence.kap.event.handle;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
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
import io.kylingence.kap.event.model.AddSegmentEvent;
import io.kylingence.kap.event.model.EventContext;


public class AddSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddSegmentHandler.class);

    @Override
    protected void onJobSuccess(EventContext eventContext) throws Exception {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        String cubePlanName = event.getCubePlanName();
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(cubePlanName);
        updateDataLoadingRange(df);
    }

    @Override
    protected AbstractExecutable createJob(EventContext eventContext) throws Exception {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);

        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        AbstractExecutable job;
        List<NCuboidLayout> layouts = new ArrayList<>();
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);
        if (CollectionUtils.isEmpty(readySegments)) {
            layouts.addAll(df.getCubePlan().getAllCuboidLayouts());
        } else {
            for (Map.Entry<Long, NDataCuboid> cuboid : readySegments.getLatestReadySegment().getCuboidsMap().entrySet()) {
                if (cuboid.getValue().getStatus() == SegmentStatusEnum.READY) {
                    layouts.add(cuboid.getValue().getCuboidLayout());
                }
            }
        }

        List<Integer> segmentIds = event.getSegmentIds();
        Set<NDataSegment> segments = Sets.newHashSet();

        if (CollectionUtils.isEmpty(segmentIds)) {
            return null;
        }
        NDataSegment dataSegment;
        for (Integer segmentId : segmentIds) {
            dataSegment = df.getSegment(segmentId);
            if (dataSegment != null) {
                segments.add(dataSegment);
            }
        }

        job = NSparkCubingJob.create(segments,
                Sets.<NCuboidLayout> newLinkedHashSet(layouts), "ADMIN");

        return job;
    }

    @Override
    public Class<?> getEventClassType() {
        return AddSegmentEvent.class;
    }
}
