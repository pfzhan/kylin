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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.EventContext;
import lombok.val;

public class AddCuboidHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddCuboidHandler.class);

    @Override
    public AbstractExecutable createJob(EventContext eventContext) {
        AddCuboidEvent event = (AddCuboidEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        String cubePlanName = event.getCubePlanName();
        NCubePlan cubePlan = NCubePlanManager.getInstance(kylinConfig, project).getCubePlan(cubePlanName);
        checkNotNull(cubePlan);
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(cubePlanName);

        val readySegs = df.getSegments(SegmentStatusEnum.READY);
        if (readySegs.isEmpty()) {
            logger.trace("no job will run for event {} because no ready segment is found", event);
            return null;
        }

        // be process layouts = all layouts - ready layouts
        val lastReadySeg = readySegs.getLatestReadySegment();
        Set<NCuboidLayout> toBeProcessedLayouts = Sets.newLinkedHashSet();
        for (NCuboidLayout layout : cubePlan.getAllCuboidLayouts()) {
            NDataCuboid nc = lastReadySeg.getCuboid(layout.getId());
            if (nc == null) {
                toBeProcessedLayouts.add(layout);
            }
        }

        if (CollectionUtils.isEmpty(toBeProcessedLayouts)) {
            logger.trace("no job will run for event {} because no layout awaits building", event);
            return null;
        }

        return NSparkCubingJob.create(Sets.newLinkedHashSet(readySegs), toBeProcessedLayouts, event.getOwner(), event.getJobId());
    }

}