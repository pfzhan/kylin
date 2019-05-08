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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;

public class NJoinProposer extends NAbstractModelProposer {

    public NJoinProposer(NSmartContext.NModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void doPropose(NDataModel modelDesc) {
        ModelTree modelTree = modelContext.getModelTree();
        Map<String, JoinTableDesc> joinTables = new HashMap<>();
        Map<TableRef, String> tableAliasMap = modelTree.getTableRefAliasMap();
        Map<String, TableRef> aliasRefMap = Maps.newHashMap();

        for (JoinTableDesc joinTableDesc : modelDesc.getJoinTables()) {
            joinTables.put(joinTableDesc.getAlias(), joinTableDesc);
            tableAliasMap.put(modelDesc.findTable(joinTableDesc.getAlias()), joinTableDesc.getAlias());
        }

        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.joins.isEmpty() || !isValidOlapContext(ctx)) {
                continue;
            }

            try {
                Map<String, JoinTableDesc> tmpJoinTablesMap = new HashMap<>();
                GreedyModelTreesBuilder.TreeBuilder.mergeContext(ctx, tmpJoinTablesMap, tableAliasMap, aliasRefMap);
                joinTables.putAll(tmpJoinTablesMap);
            } catch (Exception e) {
                val accelerateInfoMap = modelContext.getSmartContext().getAccelerateInfoMap();
                AccelerateInfo accelerateInfo = accelerateInfoMap.get(ctx.sql);
                Preconditions.checkNotNull(accelerateInfo);
                accelerateInfo.setFailedCause(e);
            }
        }

        modelDesc.setJoinTables(new ArrayList<>(joinTables.values()));
    }

}
