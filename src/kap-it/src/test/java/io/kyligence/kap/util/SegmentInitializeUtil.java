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

package io.kyligence.kap.util;

import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;

import io.kyligence.kap.job.manager.JobManager;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class SegmentInitializeUtil {

    public static void prepareSegment(KylinConfig config, String project, String dfId, String start, String end,
            boolean needRemoveExist) {
        val jobManager = JobManager.getInstance(config, project);
        val dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow df = dataflowManager.getDataflow(dfId);
        // remove the existed seg
        if (needRemoveExist) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            df = dataflowManager.updateDataflow(update);
        }
        val newSeg = dataflowManager.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)));

        // add first segment
        val jobId = jobManager.addSegmentJob(new JobParam(newSeg, df.getModel().getUuid(), "ADMIN"));
        JobFinishHelper.waitJobFinish(config, project, jobId, 240 * 1000);

        val df2 = dataflowManager.getDataflow(df.getUuid());
        val cuboidsMap2 = df2.getLastSegment().getLayoutsMap();
        Assert.assertEquals(df2.getIndexPlan().getAllLayouts().size(), cuboidsMap2.size());
        Assert.assertEquals(
                df.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId).sorted(Comparator.naturalOrder())
                        .map(a -> a + "").collect(Collectors.joining(",")),
                cuboidsMap2.keySet().stream().sorted(Comparator.naturalOrder()).map(a -> a + "")
                        .collect(Collectors.joining(",")));
    }
}
