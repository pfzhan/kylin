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

package org.apache.kylin.metadata.lookup;

import java.io.IOException;
import java.util.Comparator;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

/**
 * @author yangli9
 * 
 */
public class LookupStringTable extends LookupTable<String> {

    private static final Comparator<String> dateStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            long l1 = Long.parseLong(o1);
            long l2 = Long.parseLong(o2);
            return Long.compare(l1, l2);
        }
    };

    private static final Comparator<String> numStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            double d1 = Double.parseDouble(o1);
            double d2 = Double.parseDouble(o2);
            return Double.compare(d1, d2);
        }
    };

    private static final Comparator<String> defaultStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };

    boolean[] colIsDateTime;
    boolean[] colIsNumber;

    public LookupStringTable(TableDesc tableDesc, String[] keyColumns, IReadableTable table) throws IOException {
        super(tableDesc, keyColumns, table);
    }

    @Override
    protected void init() throws IOException {
        ColumnDesc[] cols = tableDesc.getColumns();
        colIsDateTime = new boolean[cols.length];
        colIsNumber = new boolean[cols.length];
        for (int i = 0; i < cols.length; i++) {
            DataType t = cols[i].getType();
            colIsDateTime[i] = t.isDateTimeFamily();
            colIsNumber[i] = t.isNumberFamily();
        }

        super.init();
    }

    @Override
    protected String[] convertRow(String[] cols) {
        for (int i = 0; i < cols.length; i++) {
            if (colIsDateTime[i]) {
                if (cols[i] != null)
                    cols[i] = String.valueOf(DateFormat.stringToMillis(cols[i]));
            }
        }
        return cols;
    }

    @Override
    protected Comparator<String> getComparator(int idx) {
        if (colIsDateTime[idx])
            return dateStrComparator;
        else if (colIsNumber[idx])
            return numStrComparator;
        else
            return defaultStrComparator;
    }

    @Override
    protected String toString(String cell) {
        return cell;
    }

    public Class<?> getType() {
        return String.class;
    }

}
