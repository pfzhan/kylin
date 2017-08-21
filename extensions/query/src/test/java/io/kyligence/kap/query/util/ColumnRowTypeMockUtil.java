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

import javax.annotation.Nullable;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class ColumnRowTypeMockUtil {
    public static ColumnRowType mock(String tablename, String aliasname, Pair<String, String>... columnNamesAndTypes) {
        TableDesc tableDesc = TableDesc.mockup(tablename);
        List<ColumnDesc> columnDescList = Lists.newArrayList();
        for (int i = 0; i < columnNamesAndTypes.length; i++) {
            columnDescList.add(ColumnDesc.mockup(tableDesc, 0, columnNamesAndTypes[i].getFirst(),
                    columnNamesAndTypes[i].getSecond()));
        }
        tableDesc.setColumns(columnDescList.toArray(new ColumnDesc[0]));
        final TableRef tableRef = TblColRef.tableForUnknownModel(aliasname, tableDesc);

        List<TblColRef> tblColRefs = Lists.transform(columnDescList, new Function<ColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(@Nullable ColumnDesc input) {
                return TblColRef.columnForUnknownModel(tableRef, input);
            }
        });

        ColumnRowType columnRowType = new ColumnRowType(tblColRefs);
        return columnRowType;
    }
}
