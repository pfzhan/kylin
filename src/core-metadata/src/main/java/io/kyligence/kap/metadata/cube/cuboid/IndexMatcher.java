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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableCollection;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableMultimap;
import io.kyligence.kap.guava20.shaded.common.collect.Iterables;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

public abstract class IndexMatcher {

    final SQLDigest sqlDigest;
    final NDataModel model;
    final Set<String> excludedTables;
    final boolean isBatchFusionModel;

    final ChooserContext chooserContext;

    final Map<TblColRef, Integer> tblColMap;
    final Map<String, List<Integer>> primaryKeyColumnIds;
    final Map<String, List<Integer>> foreignKeyColumnIds;
    final ImmutableMultimap<Integer, Integer> fk2Pk;

    final Map<Integer, DeriveInfo> toManyDerivedInfoMap = Maps.newHashMap();

    IndexMatcher(SQLDigest sqlDigest, ChooserContext chooserContext, Set<String> excludedTables) {
        this.sqlDigest = sqlDigest;
        this.model = chooserContext.getModel();
        this.chooserContext = chooserContext;
        this.excludedTables = excludedTables;
        this.isBatchFusionModel = chooserContext.isBatchFusionModel();

        this.fk2Pk = chooserContext.getFk2Pk();
        this.tblColMap = chooserContext.getTblColMap();
        this.primaryKeyColumnIds = chooserContext.getPrimaryKeyColumnIds();
        this.foreignKeyColumnIds = chooserContext.getForeignKeyColumnIds();

        // suppose: A join B && A join C, the relation of A->C is TO_MANY and C need to derive,
        // then the built index of this join relation only based on the flat table of A join B,
        // in order to get the correct result, the query result must join the snapshot of C.
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, model.getJoinTables(), model);
        model.getJoinTables().forEach(joinTableDesc -> {
            if (checker.getExcludedLookups().contains(joinTableDesc.getTable())) {
                JoinDesc join = joinTableDesc.getJoin();
                if (!joinTableDesc.isToManyJoinRelation() || !needJoinSnapshot(join)) {
                    return;
                }
                int foreignKeyId = foreignKeyColumnIds.get(joinTableDesc.getAlias()).get(0);
                int primaryKeyId = primaryKeyColumnIds.get(joinTableDesc.getAlias()).get(0);
                toManyDerivedInfoMap.put(primaryKeyId,
                        new DeriveInfo(DeriveInfo.DeriveType.LOOKUP, join, Lists.newArrayList(foreignKeyId), false));
            }
        });
    }

    abstract MatchResult match(LayoutEntity layout);

    void goThruDerivedDims(final IndexEntity indexEntity, Map<Integer, DeriveInfo> needDeriveCollector,
            Set<Integer> unmatchedDims) {
        Iterator<Integer> unmatchedDimItr = unmatchedDims.iterator();
        while (unmatchedDimItr.hasNext()) {
            Integer unmatchedDim = unmatchedDimItr.next();
            if (model.isLookupTable(unmatchedDim) && model.isQueryDerivedEnabled(unmatchedDim) //
                    && goThruDerivedDimsFromLookupTable(indexEntity, needDeriveCollector, unmatchedDimItr,
                            unmatchedDim)) {
                continue;
            }

            // in some rare cases, FK needs to be derived from PK
            goThruDerivedDimsFromFactTable(indexEntity, needDeriveCollector, unmatchedDimItr, unmatchedDim);

        }

        needDeriveCollector.putAll(toManyDerivedInfoMap);
    }

    private boolean needJoinSnapshot(JoinDesc join) {
        List<JoinDesc> sqlDigestJoins = sqlDigest.joinDescs == null ? Lists.newArrayList() : sqlDigest.joinDescs;
        for (JoinDesc digestJoin : sqlDigestJoins) {
            Set<TblColRef> digestPKs = Sets.newHashSet(digestJoin.getPrimaryKeyColumns());
            Set<TblColRef> digestFKs = Sets.newHashSet(digestJoin.getForeignKeyColumns());
            Set<TblColRef> joinPKs = Sets.newHashSet(join.getPrimaryKeyColumns());
            Set<TblColRef> joinFKs = Sets.newHashSet(join.getForeignKeyColumns());
            if (!CollectionUtils.isEmpty(digestFKs) && !CollectionUtils.isEmpty(digestPKs)
                    && !CollectionUtils.isEmpty(joinFKs) && !CollectionUtils.isEmpty(joinPKs)
                    && digestFKs.containsAll(joinFKs) && digestPKs.containsAll(joinPKs)
                    && joinFKs.containsAll(digestFKs) && joinPKs.containsAll(digestPKs)) {
                return true;
            }
        }
        return false;
    }

    private void goThruDerivedDimsFromFactTable(IndexEntity indexEntity, Map<Integer, DeriveInfo> needDeriveCollector,
            Iterator<Integer> unmatchedDimItr, Integer unmatchedDim) {
        ImmutableCollection<Integer> pks = fk2Pk.get(unmatchedDim);
        Iterable<Integer> pksOnIndex = Iterables.filter(pks, indexEntity::dimensionsDerive);
        Integer pk = Iterables.getFirst(pksOnIndex, null);
        if (pk != null) {
            JoinDesc joinByPKSide = model.getJoinByPKSide(pk);
            Preconditions.checkNotNull(joinByPKSide);

            //cannot derived fk from pk when left join
            if (!joinByPKSide.isInnerJoin()) {
                return;
            }
            needDeriveCollector.put(unmatchedDim,
                    new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide, Lists.newArrayList(pk), true));
            unmatchedDimItr.remove();
        }
    }

    private boolean goThruDerivedDimsFromLookupTable(IndexEntity indexEntity,
            Map<Integer, DeriveInfo> needDeriveCollector, Iterator<Integer> unmatchedDimItr, Integer unmatchedDim) {
        JoinDesc joinByPKSide = model.getJoinByPKSide(unmatchedDim);
        Preconditions.checkNotNull(joinByPKSide);
        val alias = joinByPKSide.getPKSide().getAlias();
        List<Integer> foreignKeyColumns = foreignKeyColumnIds.get(alias);
        List<Integer> primaryKeyColumns = primaryKeyColumnIds.get(alias);

        val tables = model.getAliasMap();
        if (joinByPKSide.isInnerJoin() && primaryKeyColumns.contains(unmatchedDim)) {
            Integer relatedCol = foreignKeyColumns.get(primaryKeyColumns.indexOf(unmatchedDim));
            if (indexEntity.dimensionsDerive(relatedCol)) {
                needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide,
                        Lists.newArrayList(relatedCol), true));
                unmatchedDimItr.remove();
                return true;
            }
        } else if (indexEntity.dimensionsDerive(foreignKeyColumns) && model.getColRef(unmatchedDim) != null
                && Optional.ofNullable(tables.get(alias))
                        .map(ref -> StringUtils.isNotEmpty(ref.getTableDesc().getLastSnapshotPath())).orElse(false)) {

            DeriveInfo.DeriveType deriveType = matchNonEquiJoinFks(indexEntity, joinByPKSide)
                    ? DeriveInfo.DeriveType.LOOKUP_NON_EQUI
                    : DeriveInfo.DeriveType.LOOKUP;
            needDeriveCollector.put(unmatchedDim, new DeriveInfo(deriveType, joinByPKSide, foreignKeyColumns, false));
            unmatchedDimItr.remove();
            return true;
        }
        return false;
    }

    private boolean matchNonEquiJoinFks(final IndexEntity indexEntity, final JoinDesc joinDesc) {
        return joinDesc.isNonEquiJoin() && indexEntity.dimensionsDerive(
                Stream.of(SCD2NonEquiCondSimplification.INSTANCE.extractFksFromNonEquiJoinDesc(joinDesc))
                        .map(tblColMap::get).collect(Collectors.toList()));
    }

    @Getter
    @AllArgsConstructor
    @RequiredArgsConstructor
    public static class MatchResult {

        @NonNull
        boolean isMatched;

        Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();

        CapabilityResult.IncapableCause cases;

        public List<CapabilityResult.CapabilityInfluence> influences = Lists.newArrayList();

        public MatchResult(boolean isMatched, Map<Integer, DeriveInfo> needDerive) {
            this.isMatched = isMatched;
            this.needDerive = needDerive;
        }
    }
}
