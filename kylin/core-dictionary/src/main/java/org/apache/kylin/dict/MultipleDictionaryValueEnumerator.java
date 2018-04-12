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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.datatype.DataType;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 10/28/15.
 */
@SuppressWarnings("rawtypes")
public class MultipleDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    private List<Integer> curKeys = Lists.newArrayList();
    private String curValue = null;
    private List<Dictionary<String>> dictionaryList;
    private DataType dataType;

    public MultipleDictionaryValueEnumerator(DataType dataType, List<DictionaryInfo> dictionaryInfoList) {
        this.dataType = dataType;
        dictionaryList = Lists.newArrayListWithCapacity(dictionaryInfoList.size());
        for (DictionaryInfo dictInfo : dictionaryInfoList) {
            Dictionary<String> dictionary = (Dictionary<String>) dictInfo.getDictionaryObject();
            dictionaryList.add((Dictionary<String>) dictInfo.getDictionaryObject());
            curKeys.add(dictionary.getMinId());
        }
    }

    @Override
    public String current() throws IOException {
        return curValue;
    }

    @Override
    public boolean moveNext() throws IOException {
        String minValue = null;
        int curDictIndex = 0;
        
        // multi-merge dictionary forest
        for (int i = 0; i < dictionaryList.size(); i++) {
            Dictionary<String> dict = dictionaryList.get(i);
            if (dict == null)
                continue;
            
            int curKey = curKeys.get(i);
            if (curKey > dict.getMaxId())
                continue;
            
            String curValue = dict.getValueFromId(curKey);
            if (minValue == null || dataType.compare(minValue, curValue) > 0) {
                minValue = curValue;
                curDictIndex = i;
            }
        }
        
        if (minValue == null) {
            curValue = null;
            return false;
        }
        
        curValue = minValue;
        curKeys.set(curDictIndex, curKeys.get(curDictIndex) + 1);
        return true;
    }

    @Override
    public void close() throws IOException {
    }
}
