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
package io.kyligence.kap.metadata.model.util.scd2;

import static org.apache.kylin.metadata.model.NonEquiJoinCondition.SimplifiedNonEquiJoinCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;

import io.kyligence.kap.metadata.model.NDataModel;

/**
 * do something check on scd2 condition
 */
public class SCD2CondChecker {

    public static final SCD2CondChecker INSTANCE = new SCD2CondChecker();

    /**
     * SCD2 must have  equi condition
     * @return
     */
    public boolean checkSCD2EquiJoinCond(String[] fks, String[] pks) {

        if (fks.length > 0 && fks.length == pks.length) {
            return true;

        }
        return false;

    }

    public boolean isScd2Model(NDataModel nDataModel) {
        if (nDataModel.getJoinTables().stream().filter(joinTableDesc -> joinTableDesc.getJoin().isNonEquiJoin())
                .count() == 0) {
            return false;
        }

        return nDataModel.getJoinTables().stream().filter(joinTableDesc -> joinTableDesc.getJoin().isNonEquiJoin())
                .allMatch(joinTableDesc -> SCD2NonEquiCondSimplification.INSTANCE
                        .simplifiedSCD2CondConvertChecker(joinTableDesc.getJoin()));
    }

    public boolean checkFkPkPairUnique(SimplifiedJoinDesc joinDesc) {
        return checkFkPkPairUnique(SCD2NonEquiCondSimplification.INSTANCE.simplifyFksPks(joinDesc.getForeignKey(),
                joinDesc.getPrimaryKey()), joinDesc.getSimplifiedNonEquiJoinConditions());
    }

    public boolean checkSCD2NonEquiJoinCondPair(final List<SimplifiedNonEquiJoinCondition> simplified) {
        if (CollectionUtils.isEmpty(simplified)) {
            return false;
        }

        int size = simplified.size();

        if (size % 2 != 0) {
            return false;
        }

        Map<String, Integer> mappingCount = new HashMap<>();

        for (SimplifiedNonEquiJoinCondition cond : simplified) {
            if (!checkSCD2SqlOp(cond.getOp())) {
                return false;
            }

            int inrc = cond.getOp() == SqlKind.GREATER_THAN_OR_EQUAL ? 1 : -1;

            mappingCount.put(cond.getForeignKey(), mappingCount.getOrDefault(cond.getForeignKey(), 0) + inrc);

        }

        return !mappingCount.values().stream().anyMatch(count -> count != 0);

    }

    boolean checkFkPkPairUnique(List<SimplifiedNonEquiJoinCondition> equiFkPks,
            List<SimplifiedNonEquiJoinCondition> nonEquiFkPks) {
        List<SimplifiedNonEquiJoinCondition> allFkPks = new ArrayList<>();
        allFkPks.addAll(equiFkPks);
        allFkPks.addAll(nonEquiFkPks);

        HashSet pairSet = new HashSet<String>();

        for (int i = 0; i < allFkPks.size(); i++) {
            String key = allFkPks.get(i).getForeignKey() + allFkPks.get(i).getPrimaryKey();
            if (pairSet.contains(key)) {
                return false;
            }
            pairSet.add(key);

        }
        return true;

    }

    /**
     * SCD2 only support =, >=, <
     * @param op
     * @return
     */
    private boolean checkSCD2SqlOp(SqlKind op) {

        if (Objects.isNull(op)) {
            return false;
        }

        switch (op) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
            return true;
        default:
            return false;
        }

    }
}
