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

import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kylingence.kap.event.model.EventContext;
import io.kylingence.kap.event.model.RemoveCuboidByIdEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoveCuboidByIdHandler extends AbstractEventHandler {
    @Override
    protected void doHandle(EventContext eventContext) throws Exception {
        val event = (RemoveCuboidByIdEvent) eventContext.getEvent();
        String project = event.getProject();
        KylinConfig kylinConfig = eventContext.getConfig();

        removeById(event, kylinConfig, project);

        log.info("RemoveCuboidByIdHandler doHandle success...");
    }

    private void removeById(final RemoveCuboidByIdEvent event, KylinConfig kylinConfig, String project)
            throws IOException {
        val dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        val df = dfMgr.getDataflow(event.getCubePlanName());
        val cpMgr = NCubePlanManager.getInstance(kylinConfig, project);
        cpMgr.updateCubePlan(event.getCubePlanName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                copyForWrite.removeLayouts(Sets.newHashSet(event.getLayoutIds()),
                        new Predicate2<NCuboidLayout, NCuboidLayout>() {
                            @Override
                            public boolean apply(NCuboidLayout o1, NCuboidLayout o2) {
                                return o1.equals(o2);
                            }
                        });
            }
        });
        dfMgr.removeLayouts(df, event.getLayoutIds());
    }

    @Override
    public Class<?> getEventClassType() {
        return RemoveCuboidByIdEvent.class;
    }
}
