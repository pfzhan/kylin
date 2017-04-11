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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.modeling.smart.domain.Domain;
import io.kyligence.kap.modeling.smart.domain.IDomainBuilder;

public class QueryDomainBuilder implements IDomainBuilder {
    private final QueryStats queryStats;
    private final CubeDesc origCubeDesc;

    public QueryDomainBuilder(QueryStats queryStats, CubeDesc origCubeDesc) {
        this.queryStats = queryStats;
        this.origCubeDesc = origCubeDesc;
    }

    @Override
    public Domain build() {
        long allColumnMask = queryStats.getColumnBitmap();
        RowKeyColDesc[] rowKeyCols = origCubeDesc.getRowkey().getRowKeyColumns();

        List<TblColRef> dimensionCols = new ArrayList<>();
        for (int i = 0; allColumnMask > 0 && i < rowKeyCols.length; i++) {
            if ((allColumnMask & 1) == 1) {
                dimensionCols.add(rowKeyCols[rowKeyCols.length - i - 1].getColRef());
            }
            allColumnMask >>= 1;
        }

        return new Domain(origCubeDesc.getModel(), dimensionCols, queryStats.getMeasures());
    }
}
