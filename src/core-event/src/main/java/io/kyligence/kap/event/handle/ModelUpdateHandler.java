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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AccelerateEvent;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.PostModelSemanticUpdateEvent;
import io.kyligence.kap.event.model.RemoveCuboidBySqlEvent;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.model.ModelTree;

public class ModelUpdateHandler extends AbstractEventHandler implements DeriveEventMixin {

    private static final Logger logger = LoggerFactory.getLogger(ModelUpdateHandler.class);

    @Override
    public void doHandle(EventContext eventContext) throws Exception {
        AccelerateEvent event = (AccelerateEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();
        boolean eventAutoApproved = kylinConfig.getEventAutoApproved();
        List<String> sqlList = event.getSqlPatterns();
        if (sqlList == null || sqlList.size() == 0) {
            fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
            return;
        }
        if (CollectionUtils.isNotEmpty(sqlList)) {
            // need parse sql
            NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));
            List<NSmartContext.NModelContext> modelContexts = master.getModelContext(event.isFavoriteMark());
            if (CollectionUtils.isEmpty(modelContexts)) {
                fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
                return;
            }
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            for (NSmartContext.NModelContext modelContext : modelContexts) {

                List<String> sqls = getRelatedSqlsFromModelContext(modelContext);
                NCubePlan origCubePlan = modelContext.getOrigCubePlan();
                NCubePlan targetCubePlan = modelContext.getTargetCubePlan();
                Pair<List<Long>, List<Long>> updatedLayoutsPair = calcUpdatedLayoutIds(origCubePlan, targetCubePlan);
                List<Long> addedLayoutIds = updatedLayoutsPair.getFirst();
                AddCuboidEvent addCuboidEvent;
                RemoveCuboidBySqlEvent removeCuboidEvent;
                if (CollectionUtils.isNotEmpty(addedLayoutIds)) {
                    addCuboidEvent = new AddCuboidEvent();
                    addCuboidEvent.setApproved(eventAutoApproved);
                    addCuboidEvent.setProject(project);
                    addCuboidEvent.setModelName(targetCubePlan.getModelName());
                    addCuboidEvent.setCubePlanName(targetCubePlan.getName());
                    addCuboidEvent.setSegmentRange(event.getSegmentRange());
                    addCuboidEvent.setLayoutIds(addedLayoutIds);
                    addCuboidEvent.setParentId(event.getId());
                    addCuboidEvent.setSqlPatterns(sqls);
                    eventManager.post(addCuboidEvent);
                }

                if (!event.isFavoriteMark() && origCubePlan != null) {
                    removeCuboidEvent = new RemoveCuboidBySqlEvent();
                    removeCuboidEvent.setSqlList(Lists.newArrayList(sqls));
                    removeCuboidEvent.setApproved(eventAutoApproved);
                    removeCuboidEvent.setProject(project);
                    removeCuboidEvent.setModelName(origCubePlan.getModelName());
                    removeCuboidEvent.setCubePlanName(origCubePlan.getName());
                    removeCuboidEvent.setParentId(event.getId());
                    eventManager.post(removeCuboidEvent);
                }

            }
        }

        fireEvent(new PostModelSemanticUpdateEvent(), event, eventContext.getConfig());
    }

    private List<String> getRelatedSqlsFromModelContext(NSmartContext.NModelContext modelContext) {
        List<String> sqls = Lists.newArrayList();
        if (modelContext == null) {
            return sqls;
        }
        ModelTree modelTree = modelContext.getModelTree();
        if (modelTree == null) {
            return sqls;
        }
        Collection<OLAPContext> olapContexts = modelTree.getOlapContexts();
        if (CollectionUtils.isEmpty(olapContexts)) {
            return sqls;
        }
        Iterator<OLAPContext> iterator = olapContexts.iterator();
        while (iterator.hasNext()) {
            sqls.add(iterator.next().sql);
        }

        return sqls;
    }

    private Pair<List<Long>, List<Long>> calcUpdatedLayoutIds(NCubePlan origCubePlan, NCubePlan targetCubePlan) {
        Pair<List<Long>, List<Long>> pair = new Pair<>();
        List<Long> currentLayoutIds = new ArrayList<>();
        List<Long> toBeLayoutIds = new ArrayList<>();

        List<Long> addedLayoutIds = new ArrayList<>();
        List<Long> removedLayoutIds = new ArrayList<>();
        pair.setFirst(addedLayoutIds);
        pair.setSecond(removedLayoutIds);

        if (origCubePlan == null && targetCubePlan == null) {
            return pair;
        }

        currentLayoutIds.addAll(getLayoutIds(origCubePlan));
        toBeLayoutIds.addAll(getLayoutIds(targetCubePlan));


        addedLayoutIds.addAll(currentLayoutIds);
        addedLayoutIds.addAll(toBeLayoutIds);
        addedLayoutIds.removeAll(currentLayoutIds);

        removedLayoutIds.addAll(currentLayoutIds);
        removedLayoutIds.addAll(toBeLayoutIds);
        removedLayoutIds.removeAll(toBeLayoutIds);

        return pair;
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

    @Override
    public Class<?> getEventClassType() {
        return AccelerateEvent.class;
    }
}
