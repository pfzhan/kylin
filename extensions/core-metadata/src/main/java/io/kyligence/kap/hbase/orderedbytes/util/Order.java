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
package io.kyligence.kap.hbase.orderedbytes.util;

/**
 * Used to describe or modify the lexicographical sort order of a
 * {@code byte[]}. Default ordering is considered {@code ASCENDING}. The order
 * of a {@code byte[]} can be inverted, resulting in {@code DESCENDING} order,
 * by replacing each byte with its 1's compliment.
 */
public enum Order {

    ASCENDING {
        @Override
        public int cmp(int cmp) {
            /* noop */ return cmp;
        }

        @Override
        public byte apply(byte val) {
            /* noop */ return val;
        }

        @Override
        public void apply(byte[] val) {
            /* noop */ }

        @Override
        public void apply(byte[] val, int offset, int length) {
            /* noop */ }

        @Override
        public String toString() {
            return "ASCENDING";
        }
    },

    DESCENDING {
        /**
         * A {@code byte} value is inverted by taking its 1's Complement, achieved
         * via {@code xor} with {@code 0xff}.
         */
        private static final byte MASK = (byte) 0xff;

        @Override
        public int cmp(int cmp) {
            return -1 * cmp;
        }

        @Override
        public byte apply(byte val) {
            return (byte) (val ^ MASK);
        }

        @Override
        public void apply(byte[] val) {
            for (int i = 0; i < val.length; i++) {
                val[i] ^= MASK;
            }
        }

        @Override
        public void apply(byte[] val, int offset, int length) {
            for (int i = 0; i < length; i++) {
                val[offset + i] ^= MASK;
            }
        }

        @Override
        public String toString() {
            return "DESCENDING";
        }
    };

    /**
     * Returns the adjusted trichotomous value according to the ordering imposed by this
     * {@code Order}.
     */
    public abstract int cmp(int cmp);

    /**
     * Apply order to the byte {@code val}.
     */
    public abstract byte apply(byte val);

    /**
     * Apply order to the byte array {@code val}.
     */
    public abstract void apply(byte[] val);

    /**
     * Apply order to a range within the byte array {@code val}.
     */
    public abstract void apply(byte[] val, int offset, int length);
}
