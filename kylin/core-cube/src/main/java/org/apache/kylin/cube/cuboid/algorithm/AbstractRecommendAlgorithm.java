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

package org.apache.kylin.cube.cuboid.algorithm;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRecommendAlgorithm implements CuboidRecommendAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRecommendAlgorithm.class);

    private CuboidStats cuboidStats;
    private BenefitPolicy benefitPolicy;

    private AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private AtomicBoolean canceled = new AtomicBoolean(false);

    private long timeoutMillis;

    public AbstractRecommendAlgorithm(final long timeout, BenefitPolicy benefitPolicy, CuboidStats cuboidStats) {
        if (timeout <= 0) {
            this.timeoutMillis = Long.MAX_VALUE;
        } else {
            this.timeoutMillis = timeout;
        }
        this.cuboidStats = cuboidStats;
        this.benefitPolicy = benefitPolicy;
    }

    @Override
    public void cancel() {
        cancelRequested.set(true);
    }

    /**
     * Checks whether the algorithm has been canceled or timed out.
     * 
     */
    protected boolean shouldCancel() {
        if (canceled.get()) {
            return true;
        }
        if (cancelRequested.get()) {
            canceled.set(true);
            cancelRequested.set(false);
            logger.warn("Algorithm is canceled.");
            return true;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > timeoutMillis) {
            canceled.set(true);
            logger.warn("Algorithm exceeds time limit.");
            return true;
        }
        return false;
    }

    public CuboidStats getCuboidStats() {
        return cuboidStats;
    }

    public BenefitPolicy getBenefitPolicy() {
        return benefitPolicy;
    }
}
