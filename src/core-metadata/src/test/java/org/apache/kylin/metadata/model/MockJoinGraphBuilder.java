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

package org.apache.kylin.metadata.model;

import java.util.List;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ModelNonEquiCondMock;
import io.kyligence.kap.metadata.model.NDataModel;

public class MockJoinGraphBuilder {
    private NDataModel modelDesc;
    private TableRef root;
    private List<JoinDesc> joins;

    public MockJoinGraphBuilder(NDataModel modelDesc, String rootName) {
        this.modelDesc = modelDesc;
        this.root = modelDesc.findTable(rootName);
        Assert.assertNotNull(root);
        this.joins = Lists.newArrayList();
    }

    private JoinDesc mockJoinDesc(String joinType, String[] fkCols, String[] pkCols) {
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType(joinType);
        joinDesc.setPrimaryKey(fkCols);
        joinDesc.setPrimaryKey(pkCols);
        TblColRef[] fkColRefs = new TblColRef[fkCols.length];
        for (int i = 0; i < fkCols.length; i++) {
            fkColRefs[i] = modelDesc.findColumn(fkCols[i]);
        }
        TblColRef[] pkColRefs = new TblColRef[pkCols.length];
        for (int i = 0; i < pkCols.length; i++) {
            pkColRefs[i] = modelDesc.findColumn(pkCols[i]);
        }
        joinDesc.setForeignKeyColumns(fkColRefs);
        joinDesc.setPrimaryKeyColumns(pkColRefs);
        return joinDesc;
    }

    public MockJoinGraphBuilder innerJoin(String[] fkCols, String[] pkCols) {
        joins.add(mockJoinDesc("INNER", fkCols, pkCols));
        return this;
    }

    public MockJoinGraphBuilder leftJoin(String[] fkCols, String[] pkCols) {
        joins.add(mockJoinDesc("LEFT", fkCols, pkCols));
        return this;
    }

    // simply add a col=constant condition to make a join cond non-equi
    public MockJoinGraphBuilder nonEquiLeftJoin(String pkTblName, String fkTblName, String nonEquiCol) {
        int idxTableEnd = nonEquiCol.indexOf('.');
        TableRef tableRef = modelDesc.findTable(nonEquiCol.substring(0, idxTableEnd));
        TblColRef tblColRef = tableRef.getColumn(nonEquiCol.substring(idxTableEnd + 1));
        JoinDesc joinDesc = mockJoinDesc("LEFT", new String[0], new String[0]);
        joinDesc.setPrimaryTableRef(modelDesc.findTable(pkTblName));
        joinDesc.setForeignTableRef(modelDesc.findTable(fkTblName));
        joinDesc.setNonEquiJoinCondition(ModelNonEquiCondMock.composite(SqlKind.EQUALS,
                ModelNonEquiCondMock.mockTblColRefCond(tblColRef, SqlTypeName.CHAR),
                ModelNonEquiCondMock.mockConstantCond("DUMMY", SqlTypeName.CHAR)));
        joins.add(joinDesc);
        return this;
    }

    public JoinsGraph build() {
        return new JoinsGraph(root, joins);
    }
}
