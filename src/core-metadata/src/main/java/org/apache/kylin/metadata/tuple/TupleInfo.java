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

package org.apache.kylin.metadata.tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * 
 * @author xjiang
 * 
 */
public class TupleInfo {

    private final Map<String, Integer> fieldMap;
    private final Map<TblColRef, Integer> columnMap;

    private final List<String> fields;
    private final List<TblColRef> columns;
    private final List<String> dataTypeNames;

    public TupleInfo() {
        fieldMap = new HashMap<String, Integer>();
        columnMap = new HashMap<TblColRef, Integer>();
        fields = new ArrayList<String>();
        columns = new ArrayList<TblColRef>();
        dataTypeNames = new ArrayList<String>();
    }

    public TblColRef getColumn(String fieldName) {
        int idx = getFieldIndex(fieldName);
        return columns.get(idx);
    }

    public int getColumnIndex(TblColRef col) {
        return columnMap.get(col);
    }

    public String getDataTypeName(int index) {
        return dataTypeNames.get(index);
    }

    public int getFieldIndex(String fieldName) {
        return fieldMap.get(fieldName);
    }

    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }

    public String getFieldName(TblColRef col) {
        int idx = columnMap.get(col);
        return fields.get(idx);
    }

    public boolean hasColumn(TblColRef col) {
        return columnMap.containsKey(col);
    }

    public void setField(String fieldName, TblColRef col, int index) {
        fieldMap.put(fieldName, index);

        if (col != null)
            columnMap.put(col, index);

        if (fields.size() > index)
            fields.set(index, fieldName);
        else
            fields.add(index, fieldName);

        if (columns.size() > index)
            columns.set(index, col);
        else
            columns.add(index, col);

        if (col != null) {
            if (dataTypeNames.size() > index)
                dataTypeNames.set(index, col.getColumnDesc().getUpgradedType().getName());
            else
                dataTypeNames.add(index, col.getColumnDesc().getUpgradedType().getName());
        }
    }

    public List<String> getAllFields() {
        return fields;
    }

    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public int size() {
        return fields.size();
    }

}