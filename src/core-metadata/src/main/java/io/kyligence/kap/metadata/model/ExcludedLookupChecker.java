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
package io.kyligence.kap.metadata.model;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;

@Getter
public class ExcludedLookupChecker {
    private final String factTable;
    private final Set<String> excludedLookupTables = Sets.newHashSet();
    private final Set<String> ccDependsLookupTable = Sets.newHashSet();
    private final Map<String, Set<String>> joinTableAliasMap = Maps.newHashMap();

    public ExcludedLookupChecker(Set<String> excludedTables, NDataModel model) {
        factTable = model.getRootFactTableName();
        excludedTables.forEach(table -> {
            if (table.equalsIgnoreCase(factTable)) {
                return;
            }
            excludedLookupTables.add(table);
        });
        if (model.isBroken() || CollectionUtils.isEmpty(model.getJoinTables())) {
            return;
        }

        model.getJoinTables().forEach(join -> {
            joinTableAliasMap.putIfAbsent(join.getTable(), Sets.newHashSet());
            joinTableAliasMap.get(join.getTable()).add(join.getAlias());
        });
    }

    /**
     * not very efficient for cc inner expression is a string
     */
    public boolean isColRefDependsLookupTable(TblColRef tblColRef) {
        if (!tblColRef.getColumnDesc().isComputedColumn()) {
            return excludedLookupTables.contains(tblColRef.getTableWithSchema());
        }

        String innerExpression = tblColRef.getColumnDesc().getComputedColumnExpr();
        if (ccDependsLookupTable.contains(innerExpression)) {
            return true;
        }

        for (String table : excludedLookupTables) {
            Set<String> aliasSet = joinTableAliasMap.get(table);
            if (aliasSet == null) {
                continue;
            }
            for (String alias : aliasSet) {
                String aliasWithBacktick = String.format(Locale.ROOT, "`%s`", alias);
                if (innerExpression.contains(aliasWithBacktick)) {
                    ccDependsLookupTable.add(innerExpression);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isCCDependsLookupTable(TblColRef tblColRef) {
        List<TblColRef> operands = tblColRef.getOperands();
        if (operands == null) {
            if (tblColRef.getTable() == null) {
                return false;
            } else {
                return excludedLookupTables.contains(tblColRef.getTableWithSchema());
            }
        }
        for (TblColRef colRef : operands) {
            if (isCCDependsLookupTable(colRef)) {
                return true;
            }
        }
        return false;
    }

    public boolean isMeasureOnLookupTable(FunctionDesc functionDesc) {
        List<TblColRef> colRefs = functionDesc.getColRefs();
        if (colRefs == null || colRefs.isEmpty()) {
            return false;
        }
        for (TblColRef colRef : colRefs) {
            if (colRef.isInnerColumn()) {
                if (isCCDependsLookupTable(colRef)) {
                    return true;
                }
            } else if (isColRefDependsLookupTable(colRef)) {
                return true;
            }
        }
        return false;
    }

    /**
     * For computed column is very difficult to get the excluded lookup table, so handle
     * it in the step of IndexSuggester#replaceDimOfLookupTableWithFK.
     */
    public Set<String> getUsedExcludedLookupTable(Set<TblColRef> colRefs) {
        Set<String> usedExcludedLookupTables = Sets.newHashSet();
        for (TblColRef column : colRefs) {
            if (excludedLookupTables.contains(column.getTableWithSchema())) {
                usedExcludedLookupTables.add(column.getTableWithSchema());
            }
        }
        return usedExcludedLookupTables;
    }
}