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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
public class TableRef implements Serializable {

    final transient private NDataModel model;
    final private String alias;
    final private TableDesc table;
    final private Map<String, TblColRef> columns;
    final private String modelName;

    public TableRef(NDataModel model, String alias, TableDesc table, boolean filterOutComputedColumns) {
        this.model = model;
        this.modelName = model.getName();
        this.alias = alias;
        this.table = table;
        this.columns = Maps.newLinkedHashMap();

        for (ColumnDesc col : table.getColumns()) {
            if (!filterOutComputedColumns || !col.isComputedColumn()) {
                columns.put(col.getName(), new TblColRef(this, col));
            }
        }
    }

    public NDataModel getModel() {
        return model;
    }

    public String getAlias() {
        return alias;
    }

    public TableDesc getTableDesc() {
        return table;
    }

    public String getTableName() {
        return table.getName();
    }

    public String getTableIdentity() {
        return table.getIdentity();
    }

    public TblColRef getColumn(String name) {
        return columns.get(name);
    }

    public Collection<TblColRef> getColumns() {
        return Collections.unmodifiableCollection(columns.values());
    }

    // for test only
    @Deprecated
    public TblColRef makeFakeColumn(String name) {
        ColumnDesc colDesc = new ColumnDesc();
        colDesc.setName(name);
        colDesc.setTable(table);
        return new TblColRef(this, colDesc);
    }

    // for test only
    @Deprecated
    public TblColRef makeFakeColumn(ColumnDesc colDesc) {
        return new TblColRef(this, colDesc);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TableRef t = (TableRef) o;

        if ((modelName == null ? t.modelName != null : modelName.equals(t.modelName)) == false)
            return false;
        if ((alias == null ? t.alias == null : alias.equals(t.alias)) == false)
            return false;
        if (!table.getIdentity().equals(t.table.getIdentity()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + modelName.hashCode();
        result = 31 * result + alias.hashCode();
        result = 31 * result + table.getIdentity().hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (alias.equals(table.getName()))
            return "TableRef[" + table.getName() + "]";
        else
            return "TableRef[" + alias + ":" + table.getName() + "]";
    }
}
