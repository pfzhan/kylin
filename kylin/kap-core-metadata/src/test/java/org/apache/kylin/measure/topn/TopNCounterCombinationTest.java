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

package org.apache.kylin.measure.topn;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Ignore("For collecting accuracy statistics, not for functional test")
public class TopNCounterCombinationTest extends TopNCounterTest {

    @Parameterized.Parameters
    public static Collection<Integer[]> configs() {
        return Arrays.asList(new Integer[][] {
                // with 20X space
                { 10, 20 }, // top 10%
                { 20, 20 }, // top 5%
                { 100, 20 }, // top 1%
                { 1000, 20 }, // top 0.1%

                // with 50X space
                { 10, 50 }, // top 10% 
                { 20, 50 }, // top 5% 
                { 100, 50 }, // top 1% 
                { 1000, 50 }, // top 0.1%

                // with 100X space
                { 10, 100 }, // top 10% 
                { 20, 100 }, // top 5% 
                { 100, 100 }, // top 1% 
                { 1000, 100 }, // top 0.1% 
        });
    }

    public TopNCounterCombinationTest(int keySpaceRate, int spaceSavingRate) throws Exception {
        super();
        this.TOP_K = 100;
        this.KEY_SPACE = TOP_K * keySpaceRate;
        this.SPACE_SAVING_ROOM = spaceSavingRate;
        TOTAL_RECORDS = 1000000; // 1 million
        this.PARALLEL = 10;
        this.verbose = true;
    }
}
