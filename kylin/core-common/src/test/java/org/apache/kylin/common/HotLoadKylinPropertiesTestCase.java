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
