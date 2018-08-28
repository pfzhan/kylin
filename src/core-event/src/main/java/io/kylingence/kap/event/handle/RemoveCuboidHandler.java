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


import java.util.List;

import io.kyligence.kap.cube.model.NDataCuboid;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kylingence.kap.event.model.EventContext;

public class RemoveCuboidHandler extends AbstractEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(RemoveCuboidHandler.class);

    @Override
    public void doHandle(EventContext eventContext) throws Exception {
        RemoveCuboidEvent event = (RemoveCuboidEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(event.getCubePlanName());
        List<Long> tobeRemoveCuboidLayoutIds = event.getLayoutIds();
        if (CollectionUtils.isEmpty(tobeRemoveCuboidLayoutIds)) {
            return;
        }

        // remove dataFlow cuboidLayout
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

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveCuboids(tobeRemoveCuboidLayout.toArray(new NDataCuboid[0]));
        dfMgr.updateDataflow(update);


        logger.info("RemoveCuboidHandler doHandle success...");
    }

    @Override
    public Class<?> getEventClassType() {
        return RemoveCuboidEvent.class;
    }
}
