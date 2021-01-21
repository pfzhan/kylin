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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * simplify non-equi-join
 */
public class SCD2NonEquiCondSimplification {

    public static final SCD2NonEquiCondSimplification INSTANCE = new SCD2NonEquiCondSimplification();

    /**
     * convert cond from origin model.
     * it may contain =,>=,< and so on
     *
     * if the condition is not SCD2, return null
     * @param joinDesc
     * @return
     */
    public SimplifiedJoinDesc convertToSimplifiedSCD2Cond(@Nullable JoinDesc joinDesc) throws SCD2Exception {

        if (Objects.isNull(joinDesc)) {
            return null;
        }

        NonEquiJoinCondition nonEquiJoinCondition = joinDesc.getNonEquiJoinCondition();
        //null, or is not `and` cond
        if (Objects.isNull(nonEquiJoinCondition) || nonEquiJoinCondition.getOp() != SqlKind.AND) {
            throw new SCD2Exception("scd2 must has non-equi cond and only support `AND` expression ");
        }

        SimplifiedJoinDesc convertedJoinDesc = new SimplifiedJoinDesc();

        List<SimplifiedNonEquiJoinCondition> nonEquiJoinConds = Lists.newArrayList();
        List<SimplifiedNonEquiJoinCondition> equiJoinConds = simplifyFksPks(joinDesc.getForeignKey(),
                joinDesc.getPrimaryKey());

        NonEquiJoinCondition[] nonEquiJoinConditions = nonEquiJoinCondition.getOperands();

        for (NonEquiJoinCondition nonEquivCond : nonEquiJoinConditions) {
            SimplifiedNonEquiJoinCondition scd2Cond = simplifySCD2ChildCond(nonEquivCond);

            //any child is illegal, return null
            if (Objects.isNull(scd2Cond)) {
                throw new SCD2Exception("it has illegal scd2 child expression ");
            }
            nonEquiJoinConds.add(scd2Cond);
        }

        //check >=,< pair match
        //check = join , fk and pk

        if (!SCD2CondChecker.INSTANCE.checkSCD2NonEquiJoinCondPair(nonEquiJoinConds)) {
            throw new SCD2Exception("the `>=` and `<` must be pair");
        }

        if (CollectionUtils.isEmpty(equiJoinConds)) {
            throw new SCD2Exception("scd2 should have `=` at leas one");
        }

        if (CollectionUtils.isEmpty(nonEquiJoinConds)) {
            throw new SCD2Exception("scd2 should have non-equi condition at leas one");
        }

        //check unique
        SCD2CondChecker.INSTANCE.checkFkPkPairUnique(equiJoinConds, nonEquiJoinConds);

        convertedJoinDesc.setSimplifiedNonEquiJoinConditions(nonEquiJoinConds);

        //extract fk\pk from non-equi cond
        simplifyFksPks(equiJoinConds, convertedJoinDesc);

        return convertedJoinDesc;

    }

    List<SimplifiedNonEquiJoinCondition> simplifyFksPks(String[] fks, String[] pks) {

        List<SimplifiedNonEquiJoinCondition> simplifiedFksPks = new ArrayList<>();

        for (int i = 0; i < fks.length; i++) {
            simplifiedFksPks.add(new SimplifiedNonEquiJoinCondition(fks[i], pks[i], SqlKind.EQUALS));
        }

        return simplifiedFksPks;

    }

    public TblColRef[] extractFksFromNonEquiJoinDesc(@Nonnull JoinDesc joinDesc) {
        Preconditions.checkNotNull(joinDesc, "joinDesc is null");

        List<TblColRef> fkList = convertToSimplifiedSCD2Cond(joinDesc).getSimplifiedNonEquiJoinConditions().stream()
                .map(SimplifiedNonEquiJoinCondition::getFk).distinct().collect(Collectors.toList());

        TblColRef[] fks = new TblColRef[fkList.size()];
        fkList.toArray(fks);
        return fks;
    }

    boolean simplifiedSCD2CondConvertChecker(@Nullable JoinDesc joinDesc) {
        try {
            return convertToSimplifiedSCD2Cond(joinDesc) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * non-equi join condition to fks,pks
     * @param equiJoinConds
     * @param joinDesc
     */
    private void simplifyFksPks(List<SimplifiedNonEquiJoinCondition> equiJoinConds, JoinDesc joinDesc) {

        int len = equiJoinConds.size();

        String[] fks = new String[len];
        String[] pks = new String[len];

        for (int i = 0; i < len; i++) {
            fks[i] = equiJoinConds.get(i).getForeignKey();
            pks[i] = equiJoinConds.get(i).getPrimaryKey();
        }

        joinDesc.setForeignKey(fks);
        joinDesc.setPrimaryKey(pks);

    }

    /**
     * simplify child cond
     * (a>=b) and (a<c) ->  
     *     {"a","b",">="} {"a","c","<"}
     * @param nonEquiJoinCondition
     * @return
     */
    private SimplifiedNonEquiJoinCondition simplifySCD2ChildCond(NonEquiJoinCondition nonEquiJoinCondition) {
        if (nonEquiJoinCondition.getType() != NonEquiJoinConditionType.EXPRESSION
                || nonEquiJoinCondition.getOperands().length != 2) {
            return null;
        }

        List<Pair<String, TblColRef>> fkPk = Arrays.stream(nonEquiJoinCondition.getOperands())
                .map(nonEquiJoinConditionChild -> {
                    if (nonEquiJoinConditionChild.getOp() == SqlKind.CAST
                            && nonEquiJoinConditionChild.getOperands().length == 1) {
                        return nonEquiJoinConditionChild.getOperands()[0];
                    } else {
                        return nonEquiJoinConditionChild;
                    }
                }).collect(Collectors.toList()).stream()
                .filter(nonEquiJoinConditionChild -> nonEquiJoinConditionChild.getOperands().length == 0
                        || nonEquiJoinConditionChild.getType() == NonEquiJoinConditionType.COLUMN)
                .map(nonEquiJoinCondition1 -> new Pair<String, TblColRef>(nonEquiJoinCondition1.getValue(),
                        nonEquiJoinCondition1.getColRef()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(fkPk) || fkPk.size() != 2) {
            return null;
        }

        return new SimplifiedNonEquiJoinCondition(fkPk.get(0).getFirst(), fkPk.get(0).getSecond(),
                fkPk.get(1).getFirst(), fkPk.get(1).getSecond(), nonEquiJoinCondition.getOp());
    }
}
