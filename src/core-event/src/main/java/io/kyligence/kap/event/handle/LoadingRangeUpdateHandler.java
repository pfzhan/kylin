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

import com.google.common.collect.Lists;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.event.model.LoadingRangeUpdateEvent;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.EventContext;

public class LoadingRangeUpdateHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoadingRangeUpdateHandler.class);

    @Override
    public void doHandle(EventContext eventContext) throws Exception {
        LoadingRangeUpdateEvent event = (LoadingRangeUpdateEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();
        boolean eventAutoApproved = kylinConfig.getEventAutoApproved();

        String tableName = event.getTableName();
        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }
        List<String> modelNames = NDataModelManager.getInstance(kylinConfig, project).getModelsUsingRootTable(tableDesc);
        if (CollectionUtils.isNotEmpty(modelNames)) {
            AddSegmentEvent addSegmentEvent;
            NDataflow df;
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
            for (String modelName : modelNames) {

                List<NCubePlan> matchingCubePlans = NCubePlanManager.getInstance(kylinConfig, project).findMatchingCubePlan(modelName, project, kylinConfig);
                if (CollectionUtils.isEmpty(matchingCubePlans)) {
                    continue;
                }
                for (NCubePlan cubePlan : matchingCubePlans) {
                    df = dataflowManager.getDataflow(cubePlan.getName());
                    SegmentRange segmentRange = event.getSegmentRange();
                    NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
                    addSegmentEvent = new AddSegmentEvent();
                    addSegmentEvent.setApproved(eventAutoApproved);
                    addSegmentEvent.setProject(project);
                    addSegmentEvent.setModelName(modelName);
                    addSegmentEvent.setCubePlanName(cubePlan.getName());
                    addSegmentEvent.setSegmentRange(segmentRange);
                    addSegmentEvent.setSegmentIds(Lists.newArrayList(dataSegment.getId()));
                    addSegmentEvent.setParentId(event.getId());
                    eventManager.post(addSegmentEvent);
                    logger.info("LoadingRangeUpdateHandler produce AddSegmentEvent project : {}, model : {}, cubePlan : {}, segmentRange : {}",
                            project, modelName, cubePlan.getName(), event.getSegmentRange());
                }
            }
        } else {
            // there is no models, just update the dataLoadingRange waterMark
            NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig, project);
            dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);
        }

    }

    @Override
    public Class<?> getEventClassType() {
        return LoadingRangeUpdateEvent.class;
    }
}