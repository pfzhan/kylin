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

import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.engine.spark.SegmentUtils;

public class AddSegmentHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddSegmentHandler.class);

    @Override
    public AbstractExecutable createJob(EventContext eventContext) {
        AddSegmentEvent event = (AddSegmentEvent) eventContext.getEvent();

        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);

        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        AbstractExecutable job;
        Set<NCuboidLayout> layouts = SegmentUtils.lastReadySegmentLayouts(df);
        if (CollectionUtils.isEmpty(layouts)) {
            logger.trace("no job will be created for event {} because no layout awaits building", event);
            return null;
        }

        Set<NDataSegment> segments = Sets.newHashSet();

        NDataSegment dataSegment = df.getSegment(event.getSegmentId());
        if (dataSegment != null) {
            segments.add(dataSegment);
        } else {
            logger.debug("segment {} no longer exists when creating job", event.getSegmentId());
        }

        if (CollectionUtils.isEmpty(segments)) {
            logger.debug("no job will be created for event {} because its target segment {} does not exist", event,
                    event.getSegmentId());
            return null;
        }

        job = NSparkCubingJob.create(segments, Sets.newLinkedHashSet(layouts), event.getOwner(), event.getJobId());

        return job;
    }

}
