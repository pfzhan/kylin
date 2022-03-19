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

package io.kyligence.kap.smart.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.TableAliasGenerator;

public class JoinProposer extends AbstractModelProposer {

    public JoinProposer(AbstractContext.ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void execute(NDataModel modelDesc) {

        ModelTree modelTree = modelContext.getModelTree();
        Map<String, JoinTableDesc> joinTables = new HashMap<>();

        // step 1. produce unique aliasMap
        TableAliasGenerator.TableAliasDict dict = TableAliasGenerator.generateCommonDictForSpecificModel(project);
        Map<TableRef, String> uniqueTblAliasMap = GreedyModelTreesBuilder.TreeBuilder
                .getUniqueTblAliasBasedOnPosInGraph(modelDesc.getJoinsGraph(), dict);
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.joins.isEmpty() || !isValidOlapContext(ctx)) {
                continue;
            }
            uniqueTblAliasMap.putAll(GreedyModelTreesBuilder.TreeBuilder.getUniqueTblAliasBasedOnPosInGraph(ctx, dict));
        }

        // step 2. correct table alias based on unique aliasMap and keep origin alias unchanged in model
        //                       ***** WARNING ******
        // there is one limitation that suffix of Join table alias is increase by degrees,
        //       just like f_table join lookup join lookup_1 join lookup_2
        // So ERROR Join produced if it needs to change the join relation that join alias is not obey above rule.
        Map<TableRef, String> originTblRefAlias = Maps.newHashMap();
        for (JoinTableDesc joinTable : modelDesc.getJoinTables()) {
            joinTables.put(joinTable.getAlias(), joinTable);
            originTblRefAlias.put(modelDesc.findTable(joinTable.getAlias()), joinTable.getAlias());
        }
        Map<TableRef, String> tableAliasMap = GreedyModelTreesBuilder.TreeBuilder.correctTblAliasAndKeepOriginAlias(
                uniqueTblAliasMap, modelDesc.getRootFactTable().getTableDesc(), originTblRefAlias);

        // step 3. merge context and adjust target model
        Map<String, TableRef> aliasRefMap = Maps.newHashMap();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.joins.isEmpty() || !isValidOlapContext(ctx)) {
                continue;
            }
            try {
                Map<String, JoinTableDesc> tmpJoinTablesMap = new HashMap<>();
                GreedyModelTreesBuilder.TreeBuilder.mergeContext(ctx, tmpJoinTablesMap, tableAliasMap, aliasRefMap);
                tmpJoinTablesMap.forEach((alias, joinTableDesc) -> {
                    JoinTableDesc oldJoinTable = joinTables.get(alias);
                    if (oldJoinTable != null) {
                        String flattenable = oldJoinTable.getFlattenable();
                        joinTableDesc.setFlattenable(flattenable);
                        joinTableDesc.getJoin().setType(oldJoinTable.getJoin().getType());
                    }
                });
                joinTables.putAll(tmpJoinTablesMap);
            } catch (Exception e) {
                Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
                accelerateInfoMap.get(ctx.sql).setFailedCause(e);
            }
        }

        modelDesc.setJoinTables(new ArrayList<>(joinTables.values()));
    }

}
