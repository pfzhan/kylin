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

import com.google.common.eventbus.Subscribe;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class SourceUsageUpdateListener {

    private static final Logger logger = LoggerFactory.getLogger(SourceUsageUpdateListener.class);

    private ConcurrentHashMap<String, RestClient> clientMap = new ConcurrentHashMap<>();

    @Subscribe
    public void onUpdate(SourceUsageUpdateNotifier notifier) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(kylinConfig);
        if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(kylinConfig);
            logger.debug("Start to update source usage...");
            sourceUsageManager.updateSourceUsage();
        } else {
            try {
                String owner = epochManager.getEpochOwner(EpochManager.GLOBAL).split("\\|")[0];
                if (clientMap.get(owner) == null) {
                    clientMap.clear();
                    clientMap.put(owner, new RestClient(owner));
                }
                logger.debug("Start to notify {} to update source usage", owner);
                clientMap.get(owner).updateSourceUsage();
            } catch (Exception e) {
                logger.error("Failed to update source usage using rest client", e);
            }
        }
    }
}
