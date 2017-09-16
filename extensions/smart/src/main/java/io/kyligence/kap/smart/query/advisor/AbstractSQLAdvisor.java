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

package io.kyligence.kap.smart.query.advisor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.routing.NoRealizationFoundException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.query.SQLResult;

public abstract class AbstractSQLAdvisor implements ISQLAdvisor {

    protected abstract ISQLAdviceProposer getAdviceProposer();

    protected SQLAdvice adviseSyntaxError(SQLResult sqlResult) {
        if (sqlResult.getException() != null && !(sqlResult.getException() instanceof NoRealizationFoundException)
                && !(sqlResult.getException().getCause() instanceof NoRealizationFoundException)) {
            return getAdviceProposer().propose(sqlResult);
        }
        return null;
    }

    protected Collection<OLAPTableScan> notFoundTables(Set<TableRef> allTables, OLAPContext context) {
        Set<OLAPTableScan> notFoundTables = Sets.newHashSet();
        Set<String> tableNames = Sets.newHashSet(Iterables.transform(allTables, new Function<TableRef, String>() {
            @Nullable
            @Override
            public String apply(@Nullable TableRef tableRef) {
                return tableRef.getTableIdentity();
            }
        }));
        for (OLAPTableScan olapTableScan : context.allTableScans) {
            if (!tableNames.contains(olapTableScan.getTableName())) {
                notFoundTables.add(olapTableScan);
            }
        }
        return notFoundTables;
    }

    protected Collection<TblColRef> findDimensions(Collection<TblColRef> tblColRefs, OLAPContext ctx) {
        Set<TblColRef> dimensions = Sets.newHashSet();
        Collection<TblColRef> ctxDimensions = getDimensionColumns(ctx);
        for (TblColRef tblColRef : ctxDimensions) {
            if (tblColRefs.contains(tblColRef)) {
                dimensions.add(tblColRef);
            }
        }
        return dimensions;
    }

    protected Collection<TblColRef> findMeasures(Collection<TblColRef> tblColRefs, OLAPContext ctx) {
        Set<TblColRef> measures = Sets.newHashSet();
        Collection<TblColRef> ctxDimensions = getDimensionColumns(ctx);

        for (TblColRef tblColRef : tblColRefs) {
            if (!ctxDimensions.contains(tblColRef)) {
                measures.add(tblColRef);
            }
        }
        return measures;
    }

    private Collection<TblColRef> getDimensionColumns(OLAPContext ctx) {
        Collection<TblColRef> groupByColumns = ctx.getSQLDigest().groupbyColumns;
        Collection<TblColRef> filterColumns = ctx.getSQLDigest().filterColumns;

        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }
}
