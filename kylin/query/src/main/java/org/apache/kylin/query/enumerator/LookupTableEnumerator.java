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

package org.apache.kylin.query.enumerator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.metadata.lookup.LookupStringTable;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.schema.OLAPTable;

/**
 */
public class LookupTableEnumerator implements Enumerator<Object[]> {

    private final Collection<String[]> allRows;
    private final List<ColumnDesc> colDescs;
    private final Object[] current;
    private Iterator<String[]> iterator;

    LookupTableEnumerator(OLAPContext olapContext) {
        String lookupTableName = olapContext.firstTableScan.getTableName();
        LookupStringTable table = olapContext.realization.getLookupTable(lookupTableName);

        this.allRows = table.getAllRows();

        OLAPTable olapTable = olapContext.firstTableScan.getOlapTable();
        this.colDescs = olapTable.getSourceColumns();
        this.current = new Object[colDescs.size()];

        reset();
    }

    @Override
    public boolean moveNext() {
        boolean hasNext = iterator.hasNext();
        if (hasNext) {
            String[] row = iterator.next();
            for (int i = 0, n = colDescs.size(); i < n; i++) {
                ColumnDesc colDesc = colDescs.get(i);
                int colIdx = colDesc.getZeroBasedIndex();
                if (colIdx >= 0) {
                    current[i] = Tuple.convertOptiqCellValue(row[colIdx], colDesc.getUpgradedType().getName());
                } else {
                    current[i] = null; // fake column
                }
            }
        }
        return hasNext;
    }

    @Override
    public Object[] current() {
        // NOTE if without the copy, sql_lookup/query03.sql will yields messy result. Very weird coz other lookup queries are all good.
        return Arrays.copyOf(current, current.length);
    }

    @Override
    public void reset() {
        this.iterator = allRows.iterator();
    }

    @Override
    public void close() {
    }

}
