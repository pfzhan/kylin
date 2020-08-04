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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.epoch;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.util.AddressUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class EpochOrchestrator implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(EpochOrchestrator.class);

    private KylinConfig kylinConfig;
    private EpochManager epochMgr;

    private ScheduledExecutorService checkerPool;

    private static final String OWNER_IDENTITY;

    static {
        OWNER_IDENTITY = AddressUtil.getLocalInstance() + "|" + System.currentTimeMillis();
    }

    public static String getOwnerIdentity() {
        return OWNER_IDENTITY;
    }

    public EpochOrchestrator(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        epochMgr = EpochManager.getInstance(kylinConfig);
        String serverMode = kylinConfig.getServerMode();
        if (!kylinConfig.isJobNode()) {
            logger.info("server mode: " + serverMode + ", no need to run EventOrchestrator");
            return;
        }

        long pollSecond = kylinConfig.getEpochCheckerIntervalSecond();
        logger.info("Try to update epoch every {} seconds", pollSecond);
        EpochChecker checker = new EpochChecker();
        checkerPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("EpochChecker"));
        checkerPool.scheduleWithFixedDelay(checker, 1, pollSecond, TimeUnit.SECONDS);
    }

    protected class EpochChecker implements Runnable {

        @Override
        public synchronized void run() {
            try {
                epochMgr.updateAllEpochs();
            } catch (Exception e) {
                logger.error("Failed to update epochs");
            }
        }
    }

    public void shutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.shutdownGracefully(checkerPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.forceShutdown(checkerPool);
    }

}
