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

package org.apache.kylin.metrics.lib.impl.hive;

import java.util.List;
import java.util.Properties;

import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.ReporterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which listens for new records and publishes them to hive.
 */
public class HiveReservoirReporter extends ActiveReservoirReporter {

    public static final String HIVE_REPORTER_SUFFIX = "HIVE";
    public static final HiveSink sink = new HiveSink();
    protected static final Logger logger = LoggerFactory.getLogger(HiveReservoirReporter.class);
    private final ActiveReservoir activeReservoir;
    private final HiveReservoirListener listener;

    public HiveReservoirReporter(ActiveReservoir activeReservoir, Properties props) throws Exception {
        this.activeReservoir = activeReservoir;
        this.listener = new HiveReservoirListener(props);
    }

    /**
     * Returns a new {@link Builder} for {@link HiveReservoirReporter}.
     *
     * @param activeReservoir the registry to report
     * @return a {@link Builder} instance for a {@link HiveReservoirReporter}
     */
    public static Builder forRegistry(ActiveReservoir activeReservoir) {
        return new Builder(activeReservoir);
    }

    public static String getTableFromSubject(String subject) {
        return sink.getTableFromSubject(subject);
    }

    /**
     * Starts the reporter.
     */
    public void start() {
        activeReservoir.addListener(listener);
    }

    /**
     * Stops the reporter.
     */
    public void stop() {
        activeReservoir.removeListener(listener);
    }

    /**
     * Stops the reporter.
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * A builder for {@link HiveReservoirReporter} instances.
     */
    public static class Builder extends ReporterBuilder {

        private Builder(ActiveReservoir activeReservoir) {
            super(activeReservoir);
        }

        private void setFixedProperties() {
        }

        /**
         * Builds a {@link HiveReservoirReporter} with the given properties.
         *
         * @return a {@link HiveReservoirReporter}
         */
        public HiveReservoirReporter build() throws Exception {
            setFixedProperties();
            return new HiveReservoirReporter(registry, props);
        }
    }

    private class HiveReservoirListener implements ActiveReservoirListener {

        HiveProducer producer;

        private HiveReservoirListener(Properties props) throws Exception {
            producer = new HiveProducer(props);
        }

        public boolean onRecordUpdate(final List<Record> records) {
            try {
                producer.send(records);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
            return true;
        }

        public boolean onRecordUpdate(final Record record) {
            try {
                producer.send(record);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
            return true;
        }

        public void close() {
            producer.close();
        }
    }
}
