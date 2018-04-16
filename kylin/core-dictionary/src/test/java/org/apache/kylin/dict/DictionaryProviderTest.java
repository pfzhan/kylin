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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictionaryProviderTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testReadWrite() throws Exception {
        //string dict
        Dictionary<String> dict = getDict(DataType.getType("string"),
                Arrays.asList(new String[] { "a", "b" }).iterator());
        dict.printlnStatistics();
        readWriteTest(dict);
        //number dict
        Dictionary<String> dict2 = getDict(DataType.getType("long"),
                Arrays.asList(new String[] { "1", "2" }).iterator());
        dict2.printlnStatistics();
        readWriteTest(dict2);

        //date dict
        Dictionary<String> dict3 = getDict(DataType.getType("datetime"),
                Arrays.asList(new String[] { "20161122", "20161123" }).iterator());
        dict3.printlnStatistics();
        readWriteTest(dict3);

        //date dict
        Dictionary<String> dict4 = getDict(DataType.getType("datetime"),
                Arrays.asList(new String[] { "2016-11-22", "2016-11-23" }).iterator());
        dict4.printlnStatistics();
        readWriteTest(dict4);

        //date dict
        try {
            Dictionary<String> dict5 = getDict(DataType.getType("date"),
                    Arrays.asList(new String[] { "2016-11-22", "20161122" }).iterator());
            dict5.printlnStatistics();
            readWriteTest(dict5);
            fail("Date format not correct.Should throw exception");
        } catch (IllegalArgumentException e) {
            //correct
        }
    }

    private Dictionary<String> getDict(DataType type, Iterator<String> values) throws Exception {
        IDictionaryBuilder builder = DictionaryGenerator.newDictionaryBuilder(type);
        builder.init(null, 0);
        while (values.hasNext()) {
            builder.addValue(values.next());
        }
        return builder.build();
    }

    private void readWriteTest(Dictionary<String> dict) throws Exception {
        final String path = "src/test/resources/dict/tmp_dict";
        File f = new File(path);
        f.deleteOnExit();
        f.createNewFile();
        String dictClassName = dict.getClass().getName();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
        out.writeUTF(dictClassName);
        dict.write(out);
        out.close();
        //read dict
        DataInputStream in = null;
        Dictionary<String> dict2 = null;
        try {
            File f2 = new File(path);
            in = new DataInputStream(new FileInputStream(f2));
            String dictClassName2 = in.readUTF();
            dict2 = (Dictionary<String>) ClassUtil.newInstance(dictClassName2);
            dict2.readFields(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        assertTrue(dict.equals(dict2));
    }
}
