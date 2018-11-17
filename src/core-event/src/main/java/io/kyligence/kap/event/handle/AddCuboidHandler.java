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

import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.model.NDataModel;

public class AddCuboidHandler extends AbstractEventWithJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(AddCuboidHandler.class);

    @Override
    protected void onJobSuccess(EventContext eventContext) throws Exception {
        AddCuboidEvent event = (AddCuboidEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        String cubePlanName = event.getCubePlanName();
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(cubePlanName);
        updateDataLoadingRange(df);

        List<String> sqlList = event.getSqlPatterns();
        if (CollectionUtils.isNotEmpty(sqlList)) {
            List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
            for (String sqlPattern : sqlList) {
                FavoriteQuery favoriteQuery = new FavoriteQuery(sqlPattern, sqlPattern.hashCode(), project);
                favoriteQuery.setStatus(FavoriteQueryStatusEnum.FULLY_ACCELERATED);
                favoriteQueries.add(favoriteQuery);
            }
            getFavoriteQueryDao().batchUpdateStatus(favoriteQueries);
        }

    }

    public FavoriteQueryJDBCDao getFavoriteQueryDao() {
        return FavoriteQueryJDBCDao.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @Override
    public AbstractExecutable createJob(EventContext eventContext) throws Exception {
        AddCuboidEvent event = (AddCuboidEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        Segments<NDataSegment> toBeProcessedSegments;
        Set<NCuboidLayout> toBeProcessedLayouts;
        String cubePlanName = event.getCubePlanName();
        NCubePlan cubePlan = NCubePlanManager.getInstance(kylinConfig, project).getCubePlan(cubePlanName);
        checkNotNull(cubePlan);
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(cubePlanName);

        AbstractExecutable job;
        List<Long> layoutIds = event.getLayoutIds();
        if (CollectionUtils.isEmpty(layoutIds)) {
            return null;
        }
        // calc to be process layouts
        toBeProcessedLayouts = Sets.newLinkedHashSet();
        for (Long layoutId : layoutIds) {
            NCuboidLayout cuboidLayout = cubePlan.getCuboidLayout(layoutId);
            if (cuboidLayout != null) {
                toBeProcessedLayouts.add(cuboidLayout);
            }
        }

        if (CollectionUtils.isEmpty(toBeProcessedLayouts)) {
            return null;
        }

        // calc to be process segments
        // there is no ready seg
        // case 1 : there is no seg, get segRange from loadingRage
        // case 2 : there is a seg building, get segRange from the building seg
        toBeProcessedSegments = df.getSegments();
        if (CollectionUtils.isEmpty(toBeProcessedSegments)) {
            synchronized (AddCuboidHandler.class) {
                // double check if the segment exists
                df = dfMgr.getDataflow(cubePlanName);
                toBeProcessedSegments = df.getSegments();
                if (CollectionUtils.isEmpty(toBeProcessedSegments)) {
                    List<SegmentRange> segmentRangeList = Lists.newArrayList();
                    SegmentRange segmentRange = event.getSegmentRange();
                    if (segmentRange == null) {
                        NDataModel model = cubePlan.getModel();
                        String tableName = model.getRootFactTable().getTableIdentity();
                        NDataLoadingRange dataLoadingRange = NDataLoadingRangeManager.getInstance(kylinConfig, project).getDataLoadingRange(tableName);
                        if (dataLoadingRange == null) {
                            segmentRangeList.add(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
                        } else {
                            List<SegmentRange> segmentRanges = dataLoadingRange.getSegmentRanges();
                            if (CollectionUtils.isNotEmpty(segmentRanges)) {
                                segmentRangeList.addAll(segmentRanges);
                            }
                        }

                    } else {
                        segmentRangeList.add(segmentRange);
                    }

                    if (CollectionUtils.isEmpty(segmentRangeList)) {
                        return null;
                    }
                    for (SegmentRange range : segmentRangeList) {
                        NDataSegment oneSeg = NDataflowManager.getInstance(kylinConfig, project).appendSegment(df, range);
                        toBeProcessedSegments.add(oneSeg);
                    }
                }
            }
        }

        job = NSparkCubingJob.create(Sets.newLinkedHashSet(toBeProcessedSegments), toBeProcessedLayouts
                , "ADMIN");
        return job;
    }

    @Override
    public Class<?> getEventClassType() {
        return AddCuboidEvent.class;
    }
}