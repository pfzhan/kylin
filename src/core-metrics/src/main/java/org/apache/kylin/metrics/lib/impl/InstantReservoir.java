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

package org.apache.kylin.metrics.lib.impl;

import java.util.List;

import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class InstantReservoir extends AbstractActiveReservoir {

    private static final Logger logger = LoggerFactory.getLogger(InstantReservoir.class);

    public void update(Record record) {
        if (!isReady) {
            logger.info("Current reservoir is not ready for update record");
            return;
        }
        onRecordUpdate(record);
    }

    public int size() {
        return 0;
    }

    private void onRecordUpdate(Record record) {
        boolean ifSucceed = true;
        for (ActiveReservoirListener listener : listeners) {
            if (!notifyListenerOfUpdatedRecord(listener, record)) {
                ifSucceed = false;
                logger.warn(
                        "It fails to notify listener " + listener.toString() + " of updated record " + record.getKey());
            }
        }
        if (!ifSucceed) {
            notifyListenerHAOfUpdatedRecord(record);
        }
    }

    private boolean notifyListenerOfUpdatedRecord(ActiveReservoirListener listener, Record record) {
        List<Record> recordsList = Lists.newArrayList();
        recordsList.add(record);
        return listener.onRecordUpdate(recordsList);
    }

    private boolean notifyListenerHAOfUpdatedRecord(Record record) {
        logger.info("The HA listener " + listenerHA.toString() + " for updated record " + record.getKey()
                + " will be started");
        if (!notifyListenerOfUpdatedRecord(listenerHA, record)) {
            logger.error("The HA listener also fails!!!");
            return false;
        }
        return true;
    }

}
