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


import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.LoadingRangeRefreshEvent;
import io.kylingence.kap.event.model.RefreshSegmentEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoadingRangeRefreshHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(LoadingRangeRefreshHandler.class);

    @Override
    public void doHandle(EventContext eventContext) throws Exception {
        LoadingRangeRefreshEvent event = (LoadingRangeRefreshEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();
        boolean eventAutoApproved = kylinConfig.getEventAutoApproved();

        String tableName = event.getTableName();
        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }

        SegmentRange toBeRefreshSegmentRange = event.getSegmentRange();
        if (toBeRefreshSegmentRange == null) {
            return;
        }

        NDataLoadingRange dataLoadingRange = NDataLoadingRangeManager.getInstance(kylinConfig, project).getDataLoadingRange(tableName);
        SegmentRange coveredReadySegmentRange = dataLoadingRange.getCoveredReadySegmentRange();
        if (coveredReadySegmentRange == null || !coveredReadySegmentRange.contains(toBeRefreshSegmentRange)) {
            throw new IllegalArgumentException("ToBeRefreshSegmentRange " + toBeRefreshSegmentRange + " is out of range the coveredReadySegmentRange of dataLoadingRange, the coveredReadySegmentRange is " + coveredReadySegmentRange);
        }

        List<String> modelNames = NDataModelManager.getInstance(kylinConfig, project).getModelsUsingRootTable(tableDesc);
        if (CollectionUtils.isNotEmpty(modelNames)) {
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            for (String modelName : modelNames) {
                List<NCubePlan> matchingCubePlans = NCubePlanManager.getInstance(kylinConfig, project).findMatchingCubePlan(modelName, project, kylinConfig);
                if (CollectionUtils.isNotEmpty(matchingCubePlans)) {
                    for (NCubePlan cubePlan : matchingCubePlans) {
                        RefreshSegmentEvent refreshSegmentEvent = new RefreshSegmentEvent();
                        refreshSegmentEvent.setApproved(eventAutoApproved);
                        refreshSegmentEvent.setProject(project);
                        refreshSegmentEvent.setModelName(modelName);
                        refreshSegmentEvent.setCubePlanName(cubePlan.getName());
                        refreshSegmentEvent.setSegmentRange(toBeRefreshSegmentRange);
                        refreshSegmentEvent.setParentId(event.getId());
                        eventManager.post(refreshSegmentEvent);
                        logger.info("LoadingRangeRefreshHandler produce AddSegmentEvent project : {}, model : {}, cubePlan : {}, segmentRange : {}",
                                project, modelName, cubePlan.getName(), event.getSegmentRange());
                    }
                }
            }
        }

    }

    @Override
    public Class<?> getEventClassType() {
        return LoadingRangeRefreshEvent.class;
    }

}