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
package io.kyligence.kap.query.util;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.collect.Lists;

public class ColumnRowTypeMockUtil {
    public static ColumnRowType mock(String tableName, String tableAlias,
            List<Pair<String, String>> columnNamesAndTypes) {
        TableDesc tableDesc = TableDesc.mockup(tableName);
        List<ColumnDesc> columnDescList = Lists.newArrayList();
        for (Pair<String, String> columnNamesAndType : columnNamesAndTypes) {
            columnDescList.add(
                    ColumnDesc.mockup(tableDesc, 0, columnNamesAndType.getFirst(), columnNamesAndType.getSecond()));
        }
        tableDesc.setColumns(columnDescList.toArray(new ColumnDesc[0]));
        final TableRef tableRef = TblColRef.tableForUnknownModel(tableAlias, tableDesc);

        List<TblColRef> tblColRefs = columnDescList.stream()
                .map(input -> TblColRef.columnForUnknownModel(tableRef, input)).collect(Collectors.toList());

        return new ColumnRowType(tblColRefs);
    }
}
