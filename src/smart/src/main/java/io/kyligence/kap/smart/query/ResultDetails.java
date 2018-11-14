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

package io.kyligence.kap.smart.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.Getter;

@Getter
public final class ResultDetails {
    private String queryId;
    private String sql;
    private long duration;
    private String project;
    private List<String> realizationNames = Lists.newArrayList();
    private Set<Long> cuboidIds;
    private long scannedRows;
    private long scannedBytes;
    private int resultRowCount;

    public void enrich(ResultSet resultSet) throws SQLException {
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount++;
        }
        this.resultRowCount = rowCount;
    }

    void enrich(String sql, String projectName, long duration) {
        this.sql = sql;
        this.project = projectName;
        this.duration = duration;
    }

    void enrich(Collection<OLAPContext> olapContexts) {
        this.cuboidIds = Sets.newHashSet();
        this.realizationNames = Lists.newArrayList();

        if (olapContexts == null || olapContexts.isEmpty()) {
            // TODO maybe need do something
            return;
        }

        for (OLAPContext ctx : olapContexts) {
            if (ctx.realization != null) {
                this.realizationNames.add(ctx.realization.getCanonicalName());
            }

            Long cuboid = ctx.storageContext.getCuboidId();
            if (cuboid != null) {
                this.cuboidIds.add(cuboid);
            }
        }
    }

    void enrich(QueryContext queryContext) {
        Preconditions.checkNotNull(queryContext);
        this.scannedBytes = queryContext.getScannedBytes();
        this.scannedRows = queryContext.getScannedRows();
        this.queryId = queryContext.getQueryId();
    }
}
