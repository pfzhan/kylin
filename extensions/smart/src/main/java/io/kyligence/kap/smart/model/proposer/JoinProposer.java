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

package io.kyligence.kap.smart.model.proposer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableRef;

import io.kyligence.kap.smart.model.ModelContext;

public class JoinProposer extends AbstractModelProposer {

    public JoinProposer(ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void doPropose(DataModelDesc modelDesc) {
        modelDesc.setJoinTables(modelContext.getModelTree().getJoins().values().toArray(new JoinTableDesc[0]));
        // add initial scope
        modelDesc.setDimensions(new ArrayList<ModelDimensionDesc>(0));
        modelDesc.setMetrics(new String[0]);
    }

    /**
     * get new alias by original table name, for table 'foo'
     *   foo -> foo_1
     *   foo_1 -> foo_2
     *
     * @param orginalName
     * @param oldAlias
     * @return
     */
    private static String getNewAlias(String orginalName, String oldAlias) {
        if (oldAlias.equals(orginalName)) {
            return orginalName + "_1";
        } else if (!oldAlias.startsWith(orginalName + "_")) {
            return orginalName;
        }

        String number = oldAlias.substring(orginalName.length() + 1);
        try {
            Integer i = Integer.valueOf(number);
            return orginalName + "_" + (i + 1);
        } catch (Exception e) {
            return orginalName + "_1";
        }
    }

    public List<TableRef> getTableRefByAlias(Map<TableRef, String> tableAliasMap, String alias) {
        List<TableRef> result = new ArrayList<>();
        for (Entry<TableRef, String> entry : tableAliasMap.entrySet()) {
            if (entry.getKey().equals(alias)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }
}
