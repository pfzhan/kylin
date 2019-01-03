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
package org.apache.kylin.source.datagen;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.ClassUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class DataGenTest extends NLocalFileMetadataTestCase {

    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCIConfigured() throws Exception {
        NDataModel model = getModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        ModelDataGenerator gen = getModelDataGenerator(model);
        gen.generate();

        Method method = ModelDataGenerator.class.getDeclaredMethod("path", NDataModel.class);
        method.setAccessible(true);
        String path = (String) method.invoke(gen, model);

        ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

        RawResource rawResource = store.getResource(path);

        Assert.assertTrue(rawResource != null);

        String sql = ResourceTool.cat(KylinConfig.getInstanceFromEnv(), path);

        Assert.assertTrue(StringUtils.isNotBlank(sql));
        Assert.assertTrue(sql.contains("CREATE DATABASE IF NOT EXISTS"));
        Assert.assertTrue(sql.contains("DROP TABLE IF EXISTS"));
        Assert.assertTrue(sql.contains("CREATE TABLE"));
        Assert.assertTrue(sql.contains("LOAD DATA LOCAL INPATH"));

    }

    private ModelDataGenerator getModelDataGenerator(NDataModel model) throws Exception {
        ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Class<? extends ModelDataGenerator> clz = ClassUtil
                .forName("org.apache.kylin.source.datagen.ModelDataGenerator", ModelDataGenerator.class);
        Constructor<? extends ModelDataGenerator> constructor = clz.getDeclaredConstructor(NDataModel.class, int.class,
                ResourceStore.class);
        constructor.setAccessible(true);
        ModelDataGenerator gen = constructor.newInstance(model, 100, store);
        gen.outprint = false;
        return gen;
    }

    private NDataModel getModel(String name) {
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectDefault);
        NDataModel model = mgr.getDataModelDesc(name);
        return model;
    }
}