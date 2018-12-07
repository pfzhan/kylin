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

package org.apache.kylin.common.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HiveCmdBuilderTest {

    @Before
    public void setup() {
        System.setProperty("KYLIN_CONF", TempMetadataBuilder.N_KAP_META_TEST_DATA);
    }

    @After
    public void after() throws Exception {
        System.clearProperty("kylin.source.hive.client");
        System.clearProperty("kylin.source.hive.beeline-params");
        System.clearProperty("KYLIN_CONF");
    }

    @Test
    public void testHiveCLI() {
        System.setProperty("kylin.source.hive.client", "cli");

        Map<String, String> hiveProps = new HashMap<>();
        hiveProps.put("hive.execution.engine", "mr");
        Map<String, String> hivePropsOverwrite = new HashMap<>();
        hivePropsOverwrite.put("hive.execution.engine", "tez");
        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement("USE default;");
        hiveCmdBuilder.addStatement("DROP TABLE test;");
        hiveCmdBuilder.addStatement("SHOW\n TABLES;");
        hiveCmdBuilder.setHiveConfProps(hiveProps);
        hiveCmdBuilder.overwriteHiveProps(hivePropsOverwrite);
        assertEquals("hive -e \"USE default;\nDROP TABLE test;\nSHOW\n TABLES;\n\" --hiveconf hive.execution.engine=tez", hiveCmdBuilder.build());
    }

    @Test
    public void testBeeline() throws IOException {
        String lineSeparator = java.security.AccessController.doPrivileged(new sun.security.action.GetPropertyAction("line.separator"));
        System.setProperty("kylin.source.hive.client", "beeline");
        System.setProperty("kylin.source.hive.beeline-params", "-u jdbc_url");

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.addStatement("USE default;");
        hiveCmdBuilder.addStatement("DROP TABLE test;");
        hiveCmdBuilder.addStatement("SHOW\n TABLES;");

        String cmd = hiveCmdBuilder.build();
        Assert.assertTrue(cmd.startsWith("beeline -u jdbc_url"));

        String hqlFile = cmd.substring(cmd.lastIndexOf("-f ") + 3).trim();
        hqlFile = hqlFile.substring(0, hqlFile.length() - ";exit $ret_code".length());

        String hqlStatement = FileUtils.readFileToString(new File(hqlFile), Charset.defaultCharset());
        Assert.assertEquals("USE default;" + lineSeparator + "DROP TABLE test;" + lineSeparator + "SHOW\n TABLES;" + lineSeparator, hqlStatement);

        FileUtils.forceDelete(new File(hqlFile));
    }
}