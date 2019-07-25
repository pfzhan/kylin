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

import org.apache.kylin.query.udf.stringUdf.ConcatUDF;
import org.apache.kylin.query.udf.stringUdf.InStrUDF;
import org.apache.kylin.query.udf.stringUdf.InitCapbUDF;
import org.apache.kylin.query.udf.stringUdf.LengthUDF;
import org.apache.kylin.query.udf.stringUdf.StrPosUDF;
import org.apache.kylin.query.udf.stringUdf.SubStrUDF;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class StringUDFTest {

    @Test
    public void testConcatUDF() throws Exception {
        ConcatUDF cu = new ConcatUDF();
        String str1 = cu.CONCAT("Apache ", "Kylin");
        assertTrue("Apache Kylin".equals(str1));

        String str2 = cu.CONCAT("", "Kylin");
        assertTrue("Kylin".equals(str2));

        String str3 = cu.CONCAT("Apache", "");
        assertTrue("Apache".equals(str3));
    }

    @Test
    public void testInitCapbUDF() throws Exception {
        InitCapbUDF icu = new InitCapbUDF();
        String str1 = icu.INITCAPB("abc DEF 123aVC 124Btd,lAsT");
        assertTrue("Abc Def 123avc 124btd,Last".equals(str1));

        String str2 = icu.INITCAPB("");
        assertTrue("".equals(str2));
    }

    @Test
    public void testInStrUDF() throws Exception {
        InStrUDF isd = new InStrUDF();
        int s1 = isd.INSTR("abcdebcf", "bc");
        assertTrue(s1 == 2);

        int s2 = isd.INSTR("", "bc");
        assertTrue(s2 == 0);

        int s3 = isd.INSTR("a", "bc");
        assertTrue(s3 == 0);

        int s4 = isd.INSTR("abcdebcf", "");
        assertTrue(s4 == 1);

        int s5 = isd.INSTR("abcdebcf", "bc", 4);
        assertTrue(s5 == 6);
    }

    @Test
    public void testLeftUDF() throws Exception {
        //TODO
    }

    @Test
    public void testLengthUDF() throws Exception {
        LengthUDF lu = new LengthUDF();
        int len1 = lu.LENGTH("apache kylin");
        assertTrue(len1 == 12);

        int len2 = lu.LENGTH("");
        assertTrue(len2 == 0);
    }

    @Test
    public void testStrPosUDF() throws Exception {
        StrPosUDF spu = new StrPosUDF();
        int s1 = spu.STRPOS("abcdebcf", "bc");
        assertTrue(s1 == 2);

        int s2 = spu.STRPOS("", "bc");
        assertTrue(s2 == 0);

        int s3 = spu.STRPOS("a", "bc");
        assertTrue(s3 == 0);

        int s4 = spu.STRPOS("abcdebcf", "");
        assertTrue(s4 == 1);
    }

    @Test
    public void testSubStrUDF() throws Exception {
        SubStrUDF ssu = new SubStrUDF();

        String s1 = ssu.SUBSTR("apache kylin", 2);
        assertTrue("pache kylin".equals(s1));

        String s2 = ssu.SUBSTR("apache kylin", 2, 5);
        assertTrue("pache".equals(s2));

        String s3 = ssu.SUBSTR("", 2);
        assertTrue(s3 == null);

        String s4 = ssu.SUBSTR("", 2, 5);
        assertTrue(s4 == null);

        String s5 = ssu.SUBSTR("", 0, 5);
        assertTrue(s5 == null);

        String s6 = ssu.SUBSTR("a", 1, 5);
        assertTrue("a".equals(s6));
    }

}