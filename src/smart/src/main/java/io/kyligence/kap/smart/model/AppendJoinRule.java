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

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;

import io.kyligence.kap.metadata.model.NDataModel;

public class AppendJoinRule extends AbstractJoinRule {
    /*
     * Compatible conditions:
     *  1. same fact table,
     *  2. each join is not one-to-many or many-to-many relation,
     *  3. each join from fact table is left-join,
     *  4. each join is not non-equiv-join
     */
    public boolean isCompatible(NDataModel model, ModelTree modelTree) {

        String factTableName = model.getRootFactTableName();
        if (!StringUtils.equalsIgnoreCase(factTableName, modelTree.getRootFactTable().getIdentity())) {
            return false;
        }

        for (JoinTableDesc joinTable : model.getJoinTables()) {
            if (detectIncompatible(factTableName, joinTable)) {
                return false;
            }
        }

        for (Map.Entry<String, JoinTableDesc> joinEntry : modelTree.getJoins().entrySet()) {
            if (detectIncompatible(factTableName, joinEntry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean detectIncompatible(String factTableName, JoinTableDesc joinTable) {
        JoinDesc join = joinTable.getJoin();
        if (join.isNonEquiJoin()) {
            return true;
        }

        return join.isJoinWithFactTable(factTableName) && (joinTable.isToManyJoinRelation() || !join.isLeftJoin());
    }
}