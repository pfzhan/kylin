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

import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MergeSegmentHandler extends AbstractEventWithJobHandler {

    @Override
    public AbstractExecutable createJob(Event e, String project) {
        MergeSegmentEvent event = (MergeSegmentEvent) e;

        if (!checkSubjectExists(project, event.getModelId(), event.getSegmentId(), event)) {
            return null;
        }

        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataflow(event.getModelId());
        List<LayoutEntity> layouts = Lists.newArrayList();
        for (Map.Entry<Long, NDataLayout> cuboid : df.getSegments().getLatestReadySegment().getLayoutsMap()
                .entrySet()) {
            layouts.add(cuboid.getValue().getLayout());
        }
        if (layouts.isEmpty()) {
            log.info("event {} is no longer valid because no layout awaits building", event);
            return null;
        }

        return NSparkMergingJob.merge(df.getSegment(event.getSegmentId()), Sets.newLinkedHashSet(layouts),
                event.getOwner(), event.getJobId());

    }

}
