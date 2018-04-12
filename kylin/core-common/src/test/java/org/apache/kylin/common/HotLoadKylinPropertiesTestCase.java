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

package org.apache.kylin.common;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;

/**
 * @author kangkaisen
 */

public class HotLoadKylinPropertiesTestCase {


    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();

        List<String> properties = Lists.newArrayList(
                "kylin.test.bcc.new.key=some-value",
                "kylin.engine.mr.config-override.test1=test1",
                "kylin.engine.mr.config-override.test2=test2",
                "kylin.job.lock=org.apache.kylin.job.lock.MockJobLockDup",
                "kylin.job.lock=org.apache.kylin.job.lock.MockJobLock"
        );
        cleanMetadataHelper.setUpWithSomeProperties(properties);
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }


    protected void updateProperty(String key, String value) {
        File propFile = KylinConfig.getSitePropertiesFile();
        Properties conf = new Properties();

        //load
        try (FileInputStream is = new FileInputStream(propFile)) {
            conf.load(is);
            conf.setProperty(key, value);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //store
        try (FileOutputStream out = new FileOutputStream(propFile)) {
            conf.store(out, null);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
