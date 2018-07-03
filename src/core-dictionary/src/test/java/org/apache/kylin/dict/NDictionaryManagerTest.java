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
package org.apache.kylin.dict;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NDictionaryManagerTest extends NLocalFileMetadataTestCase {

    private String projectDefault = "default";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSaveMergeDictionary() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDictionaryManager dictMgr = NDictionaryManager.getInstance(config, projectDefault);

        NDataModel dataModel = NDataModelManager.getInstance(config, projectDefault).getDataModelDesc("nmodel_basic");
        TblColRef col = dataModel.findColumn("LSTG_FORMAT_NAME");

        // non-exist input returns null;
        NDictionaryInfo nullInfo = dictMgr.buildDictionary(col, MockupReadableTable.newNonExistTable("/a/path"));
        assertEquals(null, nullInfo);

        NDictionaryInfo info1 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertEquals(3, info1.getDictionaryObject().getSize());

        // same input returns same dict
        NDictionaryInfo info2 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertTrue(info1 == info2);

        // same input values (different path) returns same dict
        NDictionaryInfo info3 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/different/path", "1", "2", "3"));
        assertTrue(info1 == info3);

        // save dictionary works in spite of non-exist table
        Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(),
                new IterableDictionaryValueEnumerator("1", "2", "3"));
        NDictionaryInfo info4 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict);
        assertTrue(info1 == info4);

        Dictionary<String> dict2 = DictionaryGenerator.buildDictionary(col.getType(),
                new IterableDictionaryValueEnumerator("1", "2", "3", "4"));
        NDictionaryInfo info5 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict2);
        assertTrue(info1 != info5);

        // merge two same dictionary
        List<NDictionaryInfo> dicts = new ArrayList<>();
        dicts.add(info2);
        dicts.add(info1);
        NDictionaryInfo mergeInfo = dictMgr.mergeDictionary(dicts);

        assertTrue(mergeInfo == info1);

        // merge two different dictionary
        dicts.clear();
        dicts.add(info1);
        dicts.add(info5);
        NDictionaryInfo mergeInfo2 = dictMgr.mergeDictionary(dicts);
        assertTrue(mergeInfo2 != info1);
        assertTrue(mergeInfo2.getCardinality() == info5.getCardinality());

        // change config to cover another logic branch
        // after set back
        // build a largest dic
        NDictionaryInfo info8 = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "4", "5", "6", "7", "8"));

        config.setProperty("kylin.dictionary.growing-enabled", "true");
        NDictionaryInfo mergeInfo3 = dictMgr.mergeDictionary(dicts);

        assertTrue(mergeInfo3 != mergeInfo2);
        assertTrue(mergeInfo3.getCardinality() == 8);

        config.setProperty("kylin.dictionary.growing-enabled", "false");

    }

}