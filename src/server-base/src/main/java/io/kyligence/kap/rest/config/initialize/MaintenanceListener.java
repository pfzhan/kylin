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

package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;
import java.io.InputStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.metadata.epoch.Epoch;
import io.kyligence.kap.metadata.epoch.EpochManager;

public class MaintenanceListener implements IKeep, EventListenerRegistry.ResourceEventListener {

    private static final Logger logger = LoggerFactory.getLogger(MaintenanceListener.class);

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        // ignore for default
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        // ignore for default
    }

    @Override
    public void onBeforeUpdate(KylinConfig config, RawResource rawResource) {
        if (!ResourceStore.GLOBAL_EPOCH.equals(rawResource.getResPath())) {
            return;
        }
        try (InputStream updateIn = rawResource.getByteSource().openStream()) {
            EpochManager manager = EpochManager.getInstance(config);
            Epoch beforeEpoch = manager.getGlobalEpoch();
            Epoch update = JsonUtil.readValue(updateIn, Epoch.class);
            if (beforeEpoch.isMaintenanceMode() && !update.isMaintenanceMode()) {
                manager.startOwnedProjects();
            }
            if (!beforeEpoch.isMaintenanceMode() && update.isMaintenanceMode()) {
                manager.shutdownOwnedProjects();
            }
        } catch (IOException e) {
            // ignore it
            logger.warn("MaintenanceListener check epoch state error", e);
        }

    }
}
