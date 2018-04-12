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

package org.apache.kylin.measure.hllc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xjiang
 */
public class HLLDistinctCountAggFunc {

    private static final Logger logger = LoggerFactory.getLogger(HLLDistinctCountAggFunc.class);

    public static HLLCounter init() {
        return null;
    }

    public static HLLCounter initAdd(Object v) {
        if (v instanceof Long) { // holistic case
            long l = (Long) v;
            return new FixedValueHLLCMockup(l);
        } else {
            HLLCounter c = (HLLCounter) v;
            return new HLLCounter(c);
        }
    }

    public static HLLCounter add(HLLCounter counter, Object v) {
        if (v instanceof Long) { // holistic case
            long l = (Long) v;
            if (counter == null) {
                return new FixedValueHLLCMockup(l);
            } else {
                if (!(counter instanceof FixedValueHLLCMockup))
                    throw new IllegalStateException("counter is not FixedValueHLLCMockup");

                ((FixedValueHLLCMockup) counter).set(l);
                return counter;
            }
        } else {
            HLLCounter c = (HLLCounter) v;
            if (counter == null) {
                return new HLLCounter(c);
            } else {
                counter.merge(c);
                return counter;
            }
        }
    }

    public static HLLCounter merge(HLLCounter counter0, Object counter1) {
        return add(counter0, counter1);
    }

    public static long result(HLLCounter counter) {
        return counter == null ? 0L : counter.getCountEstimate();
    }

    @SuppressWarnings("serial")
    public static class FixedValueHLLCMockup extends HLLCounter {

        private Long value = null;

        FixedValueHLLCMockup(long value) {
            this.value = value;
        }

        public void set(long value) {
            if (this.value == null) {
                this.value = value;
            } else {
                long oldValue = Math.abs(this.value.longValue());
                long take = Math.max(oldValue, value);
                logger.warn("Error to aggregate holistic count distinct, old value " + oldValue + ", new value " + value + ", taking " + take);
                this.value = -take; // make it obvious that this value is wrong
            }
        }

        @Override
        public void clear() {
            this.value = null;
        }

        @Override
        protected void add(long hash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void merge(HLLCounter another) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCountEstimate() {
            return value;
        }

        @Override
        public void writeRegisters(ByteBuffer out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readRegisters(ByteBuffer in) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + (int) (value ^ (value >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj))
                return false;
            if (getClass() != obj.getClass())
                return false;
            FixedValueHLLCMockup other = (FixedValueHLLCMockup) obj;
            if (!value.equals(other.value))
                return false;
            return true;
        }
    }

}
