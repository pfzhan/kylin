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

package org.apache.kylin.cube.cuboid;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author xjiang
 * 
 */
public class CombinationTest {

    public int findSmallerSibling(long valueBits, long valueMask) {
        if ((valueBits | valueMask) != valueMask) {
            throw new IllegalArgumentException("Dismatch " + Long.toBinaryString(valueBits) + " from " + Long.toBinaryString(valueMask));
        }

        int n = Long.bitCount(valueMask);
        int k = Long.bitCount(valueBits);
        long[] bitMasks = new long[n];
        long leftBits = valueMask;
        for (int i = 0; i < n; i++) {
            long lowestBit = Long.lowestOneBit(leftBits);
            bitMasks[i] = lowestBit;
            leftBits &= ~lowestBit;
        }
        return combination(valueBits, bitMasks, 0, 0L, k);
    }

    private int combination(long valueBits, long[] bitMasks, int offset, long prefix, int k) {
        if (k == 0) {
            if (prefix < valueBits) {
                System.out.println(Long.toBinaryString(prefix));
                return 1;
            } else {
                return 0;
            }
        } else {
            int count = 0;
            for (int i = offset; i < bitMasks.length; i++) {
                long newPrefix = prefix | bitMasks[i];
                if (newPrefix < valueBits) {
                    count += combination(valueBits, bitMasks, i + 1, newPrefix, k - 1);
                }
            }
            return count;
        }
    }

    private long calculateCombination(int n, int k) {
        if (n < k) {
            throw new IllegalArgumentException("N < K");
        }
        long res = 1;
        for (int i = n - k + 1; i <= n; i++) {
            res *= i;
        }
        for (int i = 1; i <= k; i++) {
            res /= i;
        }
        return res;
    }

    @Test
    public void testComb3() {
        long valueBits = 1 << 4 | 1 << 6 | 1 << 8;
        System.out.println("value = " + Long.toBinaryString(valueBits) + ", count = " + Long.bitCount(valueBits));
        long valueMask = (long) Math.pow(2, 10) - 1;
        System.out.println("mask = " + Long.toBinaryString(valueMask) + ", count = " + Long.bitCount(valueMask));
        System.out.println("************");
        int count = findSmallerSibling(valueBits, valueMask);
        System.out.println("smaller sibling count = " + count);
        int cnk = (int) calculateCombination(Long.bitCount(valueMask), Long.bitCount(valueBits));
        assertTrue(cnk > count);
    }
}
