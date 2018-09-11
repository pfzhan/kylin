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


import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.model.ModelTree;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.ModelUpdateEvent;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kylingence.kap.event.model.EventContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ModelUpdateHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(ModelUpdateHandler.class);

    @Override
    public void doHandle(EventContext eventContext) throws Exception {
        ModelUpdateEvent event = (ModelUpdateEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();
        boolean eventAutoApproved = kylinConfig.getEventAutoApproved();
        Map<String, String> sqlMap = event.getSqlMap();
        if (sqlMap == null || sqlMap.size() == 0) {
            return;
        }
        List<String> sqlList = Lists.newArrayList(sqlMap.keySet());
        if (CollectionUtils.isNotEmpty(sqlList)) {
            // need parse sql
            NSmartMaster master = new NSmartMaster(kylinConfig, project, sqlList.toArray(new String[0]));
            if (event.isFavoriteMark()) {
                master.runAll();
            } else {
                // unFavorite sql will not update model and cubePlan, just convert to a RemoveCuboidEvent and dispatch it
                master.analyzeSQLs();
                master.selectModel();
                master.selectCubePlan();

//                master.shrinkCubePlan();
//                master.shrinkModel();
            }
            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            if (CollectionUtils.isEmpty(modelContexts)) {
                return;
            }
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            for (NSmartContext.NModelContext modelContext : modelContexts) {

                List<String> sqls = getRelatedSqlsFromModelContext(modelContext);
                List<String> sqlIdList = getSqlIdList(sqls, sqlMap);
                NCubePlan origCubePlan = modelContext.getOrigCubePlan();
                NCubePlan targetCubePlan = modelContext.getTargetCubePlan();
                Pair<List<Long>, List<Long>> updatedLayoutsPair = calcUpdatedLayoutIds(origCubePlan, targetCubePlan);
                List<Long> addedLayoutIds = updatedLayoutsPair.getFirst();
                AddCuboidEvent addCuboidEvent;
                RemoveCuboidEvent removeCuboidEvent;
                if (CollectionUtils.isNotEmpty(addedLayoutIds)) {
                    addCuboidEvent = new AddCuboidEvent();
                    addCuboidEvent.setApproved(eventAutoApproved);
                    addCuboidEvent.setProject(project);
                    addCuboidEvent.setModelName(targetCubePlan.getModelName());
                    addCuboidEvent.setCubePlanName(targetCubePlan.getName());
                    addCuboidEvent.setSegmentRange(event.getSegmentRange());
                    addCuboidEvent.setLayoutIds(addedLayoutIds);
                    addCuboidEvent.setParentId(event.getId());
                    addCuboidEvent.setSqlIdList(sqlIdList);
                    eventManager.post(addCuboidEvent);
                }

                if (!event.isFavoriteMark() && origCubePlan != null) {
                    removeCuboidEvent = new RemoveCuboidEvent();
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

    private List<String> getSqlIdList(List<String> sqls, Map<String, String> sqlMap) {
        List<String> sqlIdList = Lists.newArrayList();
        if (sqls == null || sqls.size() == 0) {
            return sqlIdList;
        }

        for (String sql : sqls) {
            sqlIdList.add(sqlMap.get(sql));
        }

        return sqlIdList;
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
        return ModelUpdateEvent.class;
    }
}
