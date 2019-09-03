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

package org.apache.kylin.query.udf;

import org.apache.kylin.query.udf.otherUdf.IfUDF;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IfUDFTest {

    @Test
    public void testIfUDF() throws Exception {
        IfUDF ifUDF = new IfUDF();
        int score1 = 100, score2 = 85, score3 = 65, score4 = -10;
        assertEquals(ifUDF.IF(score1 > 0, ifUDF.IF(score1 > 90, "优秀", ifUDF.IF(score1 > 70, "良好", "中等")), "非法分数"),
                "优秀");
        assertEquals(ifUDF.IF(score2 > 0, ifUDF.IF(score2 > 90, "优秀", ifUDF.IF(score2 > 70, "良好", "中等")), "非法分数"),
                "良好");
        assertEquals(ifUDF.IF(score3 > 0, ifUDF.IF(score3 > 90, "优秀", ifUDF.IF(score3 > 70, "良好", "中等")), "非法分数"),
                "中等");
        assertEquals(ifUDF.IF(score4 > 0, ifUDF.IF(score4 > 90, "优秀", ifUDF.IF(score4 > 70, "良好", "中等")), "非法分数"),
                "非法分数");
    }
}
