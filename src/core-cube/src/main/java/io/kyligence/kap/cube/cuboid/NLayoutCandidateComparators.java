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

package io.kyligence.kap.cube.cuboid;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NLayoutCandidateComparators {

    private static final Logger logger = LoggerFactory.getLogger(NLayoutCandidateComparators.class);

    public static Comparator<NLayoutCandidate> simple() {
        return (o1, o2) -> o2.getCuboidLayout().getOrderedDimensions().size()
                - o1.getCuboidLayout().getOrderedDimensions().size();
    }

    public static Comparator<NLayoutCandidate> matchQueryPattern(final ImmutableSet<TblColRef> filters,
            KylinConfig config) {
        return new Comparator<NLayoutCandidate>() {

            @Override
            public int compare(NLayoutCandidate o1, NLayoutCandidate o2) {
                SortedSet<Integer> position1 = getFilterPositionSet(o1);
                SortedSet<Integer> position2 = getFilterPositionSet(o2);
                Iterator<Integer> iter1 = position1.iterator();
                Iterator<Integer> iter2 = position2.iterator();

                while (iter1.hasNext() && iter2.hasNext()) {
                    int i1 = iter1.next();
                    int i2 = iter2.next();

                    int c = i1 - i2;
                    if (c != 0)
                        return c;
                }

                return 0;
            }

            private SortedSet<Integer> getFilterPositionSet(final NLayoutCandidate candidate) {
                SortedSet<Integer> positions = Sets.newTreeSet();
                List<TblColRef> sortedFilterCols = Lists.newArrayList(filters);
                sortedFilterCols.sort(filterColComparator(candidate, config));

                for (TblColRef filterCol : sortedFilterCols) {
                    DeriveInfo deriveInfo = candidate.getDerivedToHostMap().get(filterCol);
                    if (deriveInfo == null) {
                        int id = candidate.getCuboidLayout().getDimensionPos(filterCol);
                        positions.add(candidate.getCuboidLayout().getColOrder().indexOf(id));
                    } else {
                        TblColRef[] hostCols = deriveInfo.columns;
                        for (TblColRef hostCol : hostCols) {
                            int id = candidate.getCuboidLayout().getDimensionPos(hostCol);
                            positions.add(candidate.getCuboidLayout().getColOrder().indexOf(id));
                        }
                    }
                }
                return positions;
            }
        };
    }

    private static Comparator<TblColRef> filterColComparator(NLayoutCandidate candidate, KylinConfig config) {
        return new Comparator<TblColRef>() {
            @Override
            public int compare(TblColRef o1, TblColRef o2) {
                // priority desc
                int res = o2.getFilterLevel().getPriority() - o1.getFilterLevel().getPriority();
                if (res == 0) {
                    NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config,
                            candidate.getCuboidLayout().getModel().getProject());
                    TableExtDesc tableExtDesc1 = tableMetadataManager
                            .getOrCreateTableExt(o1.getTableRef().getTableDesc());
                    TableExtDesc.ColumnStats ret1 = tableExtDesc1.getColumnStats()
                            .get(o1.getColumnDesc().getZeroBasedIndex());
                    TableExtDesc tableExtDesc2 = tableMetadataManager
                            .getOrCreateTableExt(o2.getTableRef().getTableDesc());
                    TableExtDesc.ColumnStats ret2 = tableExtDesc2.getColumnStats()
                            .get(o2.getColumnDesc().getZeroBasedIndex());

                    //null last
                    if (ret2 == null)
                        return (ret1 == null) ? 0 : -1;
                    if (ret1 == null)
                        return 1;
                    // getCardinality desc
                    res = Long.compare(ret2.getCardinality(), ret1.getCardinality());
                }
                return res;
            }
        };
    }
}
