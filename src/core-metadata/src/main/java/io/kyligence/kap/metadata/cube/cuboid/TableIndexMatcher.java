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
package io.kyligence.kap.metadata.cube.cuboid;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableIndexMatcher extends IndexMatcher {

    private final boolean isUseTableIndexAnswerNonRawQuery;
    private final Set<Integer> sqlColumns;

    public TableIndexMatcher(SQLDigest sqlDigest, ChooserContext chooserContext, Set<String> excludedTables,
                             boolean isUseTableIndexAnswerNonRawQuery) {
        super(sqlDigest, chooserContext, excludedTables);
        this.isUseTableIndexAnswerNonRawQuery = isUseTableIndexAnswerNonRawQuery;
        this.sqlColumns = sqlDigest.allColumns.stream().map(tblColMap::get).collect(Collectors.toSet());
    }

    public MatchResult match(LayoutEntity layout) {
        if (!needTableIndexMatch(layout.getIndex())) {
            return new MatchResult(false);
        }

        log.trace("Matching table index");
        final Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();
        Set<Integer> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlColumns);
        if (isBatchFusionModel) {
            unmatchedCols.removeAll(layout.getStreamingColumns().keySet());
        }
        unmatchedCols.removeAll(layout.getOrderedDimensions().keySet());
        goThruDerivedDims(layout.getIndex(), needDerive, unmatchedCols);
        if (!unmatchedCols.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Table index {} with unmatched columns {}", layout, unmatchedCols);
            }
            return new MatchResult(false, needDerive,
                    CapabilityResult.IncapableCause.create(CapabilityResult.IncapableType.TABLE_INDEX_MISSING_COLS),
                    Lists.newArrayList());
        }
        return new MatchResult(true, needDerive);
    }

    private boolean needTableIndexMatch(IndexEntity index) {
        return index.isTableIndex() && (sqlDigest.isRawQuery || isUseTableIndexAnswerNonRawQuery);
    }
}
