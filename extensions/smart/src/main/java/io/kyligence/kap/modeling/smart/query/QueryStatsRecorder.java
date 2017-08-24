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

package io.kyligence.kap.modeling.smart.query;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import io.kyligence.kap.query.mockup.AbstractQueryRecorder;
import io.kyligence.kap.query.mockup.QueryRecord;

public class QueryStatsRecorder extends AbstractQueryRecorder<QueryStats> {
    private final QueryStats queryStats;

    public QueryStatsRecorder() {
        queryStats = new QueryStats();
    }

    @Override
    public synchronized void record(QueryRecord record) {
        GTCubeStorageQueryRequest gtRequest = record.getGtRequest();
        CubeInstance cubeInstance = record.getCubeInstance();

        if (gtRequest == null || cubeInstance == null) {
            return;
        }

        final Cuboid cuboid = gtRequest.getCuboid();
        Collection<String> groupByCols = Collections2.transform(gtRequest.getGroups(),
                new Function<TblColRef, String>() {
                    @Override
                    public String apply(@Nullable TblColRef tblColRef) {
                        return tblColRef.getIdentity();
                    }
                });

        Collection<String> filterCols = Collections2.transform(gtRequest.getFilterCols(),
                new Function<TblColRef, String>() {
                    @Override
                    public String apply(@Nullable TblColRef tblColRef) {
                        return tblColRef.getIdentity();
                    }
                });

        Set<String> usedCols = Sets.newHashSet();
        usedCols.addAll(groupByCols);
        usedCols.addAll(filterCols);

        Set<FunctionDesc> funcs = gtRequest.getMetrics();
        DataModelDesc modelDesc = cubeInstance.getModel();
        for (FunctionDesc func : funcs) {
            func.init(modelDesc);
        }

        queryStats.addCuboid(cuboid.getId());
        queryStats.addMeasures(funcs);
        queryStats.addColPairs(usedCols);
        queryStats.addGroupBy(groupByCols);
        queryStats.addFilter(filterCols);
        queryStats.addAppear(usedCols);
        queryStats.addCuboidCols(usedCols);
        queryStats.addTotalQueries();
    }

    public QueryStats getResult() {
        return queryStats;
    }
}
