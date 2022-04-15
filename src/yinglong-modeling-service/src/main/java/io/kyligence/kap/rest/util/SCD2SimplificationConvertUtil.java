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
package io.kyligence.kap.rest.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.util.scd2.SCD2Exception;
import io.kyligence.kap.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinDesc;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;

public class SCD2SimplificationConvertUtil {

    /**
     * fill simplified cond according to joinTables
     * return true if is scd2 cond
     * @param simplifiedJoinTableDescs
     * @param joinTables
     */
    private static void fillSimplifiedCond(@Nonnull List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs,
            @Nonnull List<JoinTableDesc> joinTables) {
        Preconditions.checkNotNull(simplifiedJoinTableDescs);
        Preconditions.checkNotNull(joinTables);

        for (int i = 0; i < joinTables.size(); i++) {
            JoinTableDesc joinTableDesc = joinTables.get(i);
            JoinDesc joinDesc = joinTableDesc.getJoin();

            if (Objects.isNull(joinDesc.getNonEquiJoinCondition())) {
                continue;
            }

            try {
                SimplifiedJoinDesc convertedJoinDesc = SCD2NonEquiCondSimplification.INSTANCE
                        .convertToSimplifiedSCD2Cond(joinDesc);

                SimplifiedJoinDesc responseJoinDesc = simplifiedJoinTableDescs.get(i).getSimplifiedJoinDesc();
                responseJoinDesc
                        .setSimplifiedNonEquiJoinConditions(convertedJoinDesc.getSimplifiedNonEquiJoinConditions());
                responseJoinDesc.setForeignKey(convertedJoinDesc.getForeignKey());
                responseJoinDesc.setPrimaryKey(convertedJoinDesc.getPrimaryKey());
            } catch (SCD2Exception e) {
                throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR, "only support scd2 join condition");
            }

        }

    }

    /**
     * convert join tables to  simplified join tables, 
     * and fill non-equi join cond
     * @param joinTables
     * @return
     */
    public static List<SimplifiedJoinTableDesc> simplifiedJoinTablesConvert(List<JoinTableDesc> joinTables) {
        if (Objects.isNull(joinTables)) {
            return null;
        }

        List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;
        try {
            simplifiedJoinTableDescs = Arrays.asList(
                    JsonUtil.readValue(JsonUtil.writeValueAsString(joinTables), SimplifiedJoinTableDesc[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        fillSimplifiedCond(simplifiedJoinTableDescs, joinTables);

        return simplifiedJoinTableDescs;

    }

    /**
     * convert simplifiedJoinTables to join tables
     * @param simplifiedJoinTableDescs
     * @return
     */
    public static List<JoinTableDesc> convertSimplified2JoinTables(
            List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs) {
        if (Objects.isNull(simplifiedJoinTableDescs)) {
            return null;
        }
        List<JoinTableDesc> joinTableDescs;
        try {
            joinTableDescs = Arrays.asList(
                    JsonUtil.readValue(JsonUtil.writeValueAsString(simplifiedJoinTableDescs), JoinTableDesc[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return joinTableDescs;

    }

    public static List<JoinTableDesc> deepCopyJoinTables(List<JoinTableDesc> joinTables) {

        return convertSimplified2JoinTables(simplifiedJoinTablesConvert(joinTables));

    }
}
