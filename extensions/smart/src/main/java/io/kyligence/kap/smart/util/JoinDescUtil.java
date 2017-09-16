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

package io.kyligence.kap.smart.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.DataModelDesc.TableKind;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;

public class JoinDescUtil {

    public static JoinTableDesc convert(JoinDesc join, TableKind kind, String pkTblAlias, String fkTblAlias) {
        if (join == null) {
            return null;
        }

        TableRef table = join.getPKSide();
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setKind(kind);
        joinTableDesc.setTable(table.getTableIdentity());
        joinTableDesc.setAlias(pkTblAlias);

        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType(join.getType());
        joinDesc.setPrimaryKey(join.getPrimaryKey());

        String[] fkCols = new String[join.getForeignKey().length];
        for (int i = 0; i < fkCols.length; i++) {
            fkCols[i] = fkTblAlias + "." + join.getForeignKeyColumns()[i].getName();
        }
        joinDesc.setForeignKey(fkCols);
        joinTableDesc.setJoin(joinDesc);

        return joinTableDesc;
    }

    public static Map<JoinDesc, TableKind> resolveTableType(List<JoinDesc> joins) {
        Map<JoinDesc, TableKind> tableKindByJoins = new HashMap<>();
        Map<String, JoinDesc> fkTables = new HashMap<>();
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getFKSide();
            String tableAlias = table.getAlias();
            if (fkTables.containsKey(tableAlias)) {
                // error
            }
            fkTables.put(tableAlias, joinDesc);
        }
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getPKSide();
            String tableAlias = table.getAlias();
            if (fkTables.containsKey(tableAlias)) {
                tableKindByJoins.put(joinDesc, TableKind.FACT);
            } else {
                tableKindByJoins.put(joinDesc, TableKind.LOOKUP);
            }
        }
        return tableKindByJoins;
    }
}
