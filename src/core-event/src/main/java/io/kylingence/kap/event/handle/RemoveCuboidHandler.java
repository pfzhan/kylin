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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.RemoveCuboidEvent;

public class RemoveCuboidHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(RemoveCuboidHandler.class);

    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        final RemoveCuboidEvent event = (RemoveCuboidEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());

        if (CollectionUtils.isNotEmpty(event.getLayoutIds())) {
            final NCubePlanManager cpMgr = NCubePlanManager.getInstance(kylinConfig, project);
            cpMgr.updateCubePlan(event.getCubePlanName(), new NCubePlanManager.NCubePlanUpdater() {
                @Override
                public void modify(NCubePlan copyForWrite) {
                    cpMgr.removeLayouts(copyForWrite, Sets.newHashSet(event.getLayoutIds()),
                            new Predicate2<NCuboidLayout, NCuboidLayout>() {
                                @Override
                                public boolean apply(NCuboidLayout o1, NCuboidLayout o2) {
                                    return o1.equals(o2);
                                }
                            });
                }
            });
            removeLayouts(df, event.getLayoutIds(), dfMgr);
        }

        List<String> sqlList = event.getSqlList();
        if (CollectionUtils.isEmpty(sqlList)) {
            return;
        }

        NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));
        doShrink(master);

        List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
        if (CollectionUtils.isNotEmpty(modelContexts)) {
            for (NSmartContext.NModelContext modelContext : modelContexts) {
                NCubePlan origCubePlan = modelContext.getOrigCubePlan();
                NCubePlan targetCubePlan = modelContext.getTargetCubePlan();
                List<Long> tobeRemoveCuboidLayoutIds = calcRemovedLayoutIds(origCubePlan, targetCubePlan);
                if (CollectionUtils.isEmpty(tobeRemoveCuboidLayoutIds)) {
                    continue;
                }
                // remove dataFlow cuboidLayout
                df = removeLayouts(df, tobeRemoveCuboidLayoutIds, dfMgr);
            }
        }

        logger.info("RemoveCuboidHandler doHandle success...");
    }

    private void doShrink(NSmartMaster master) throws IOException {
        master.analyzeSQLs();
        master.selectModel();
        master.selectCubePlan();

        master.shrinkCubePlan();
        master.shrinkModel();
        master.saveCubePlan();
        master.saveModel();
    }

    private List<Long> calcRemovedLayoutIds(NCubePlan origCubePlan, NCubePlan targetCubePlan) {
        List<Long> currentLayoutIds = new ArrayList<>();
        List<Long> toBeLayoutIds = new ArrayList<>();
        List<Long> removedLayoutIds = new ArrayList<>();

        if (origCubePlan == null && targetCubePlan == null) {
            return removedLayoutIds;
        }

        currentLayoutIds.addAll(getLayoutIds(origCubePlan));
        toBeLayoutIds.addAll(getLayoutIds(targetCubePlan));

        removedLayoutIds.addAll(currentLayoutIds);
        removedLayoutIds.addAll(toBeLayoutIds);
        removedLayoutIds.removeAll(toBeLayoutIds);

        return removedLayoutIds;
    }

    private List<Long> getLayoutIds(NCubePlan cubePlan) {
        List<Long> layoutIds = Lists.newArrayList();
        if (cubePlan == null) {
            return layoutIds;
        }
        List<NCuboidLayout> layoutList = cubePlan.getAllCuboidLayouts();
        if (CollectionUtils.isEmpty(layoutList)) {
            return layoutIds;
        }

        for (NCuboidLayout layout : layoutList) {
            layoutIds.add(layout.getId());
        }
        return layoutIds;
    }

    private NDataflow removeLayouts(NDataflow df, List<Long> tobeRemoveCuboidLayoutIds, NDataflowManager dfMgr)
            throws IOException {
        List<NDataCuboid> tobeRemoveCuboidLayout = Lists.newArrayList();
        Segments<NDataSegment> segments = df.getSegments();
        for (NDataSegment segment : segments) {
            for (Long tobeRemoveCuboidLayoutId : tobeRemoveCuboidLayoutIds) {
                NDataCuboid dataCuboid = segment.getCuboid(tobeRemoveCuboidLayoutId);
                if (dataCuboid == null) {
                    continue;
                }
                tobeRemoveCuboidLayout.add(dataCuboid);
            }
        }

        if (CollectionUtils.isNotEmpty(tobeRemoveCuboidLayout)) {
            NDataflowUpdate update = new NDataflowUpdate(df.getName());
            update.setToRemoveCuboids(tobeRemoveCuboidLayout.toArray(new NDataCuboid[0]));
            return dfMgr.updateDataflow(update);
        }
        return df;
    }

    @Override
    public Class<?> getEventClassType() {
        return RemoveCuboidEvent.class;
    }
}
