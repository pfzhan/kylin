/**
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

package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapRowFilter implements IRowFilter {
    private static final Logger logger = LoggerFactory.getLogger(KapRowFilter.class);

    private Hashtable<TblColRef, String> cubeAccessControlColumns = new Hashtable<>();

    @Override
    public TupleFilter getRowFilter(OLAPAuthentication authentication, Collection<TblColRef> allCubeColumns, Map<String, String> conditions) {
        Hashtable<String, String> aclColumns = IACLMetaData.accessControlColumnsByUser.get(authentication.getUsername().toLowerCase());

        fetchCubeLimitColumns(aclColumns, allCubeColumns);

        if (0 == cubeAccessControlColumns.size()) {
            logger.error("There are no access control columns on current cube");
            return null;
        }

        TupleFilter finalFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        for (Map.Entry<TblColRef, String> condition : cubeAccessControlColumns.entrySet()) {
            TupleFilter equalFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
            TupleFilter colFilter = new ColumnTupleFilter(condition.getKey());
            TupleFilter constantFilter = new ConstantTupleFilter(condition.getValue());
            equalFilter.addChild(colFilter);
            equalFilter.addChild(constantFilter);
            finalFilter.addChild(equalFilter);
        }
        return finalFilter;
    }

    private void fetchCubeLimitColumns(Hashtable<String, String> aclColumns, Collection<TblColRef> allCubeColumns) {
        for (TblColRef cubeCol : allCubeColumns) {
            for (Map.Entry<String, String> aclCol : aclColumns.entrySet()) {
                if (cubeCol.getCanonicalName().toLowerCase().contains(aclCol.getKey()) & !isBooleanValue(aclCol.getValue())) {
                    cubeAccessControlColumns.put(cubeCol, aclCol.getValue());
                }
            }
        }
    }

    private boolean isBooleanValue(String value) {
        return (value.equalsIgnoreCase(IACLMetaData.DENY) || value.equalsIgnoreCase(IACLMetaData.YES));
    }
}
