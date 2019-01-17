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


import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;


public class NLayoutCandidateComparators {

    private NLayoutCandidateComparators() {

    }

    public static Comparator<NLayoutCandidate> simple() {
        return (o1, o2) -> o2.getCuboidLayout().getOrderedDimensions().size()
                - o1.getCuboidLayout().getOrderedDimensions().size();
    }

    public static Comparator<NLayoutCandidate> matchQueryPattern(final ImmutableSet<TblColRef> filters,
                                                                 KylinConfig config) {
        return new Comparator<NLayoutCandidate>() {

            @Override
            public int compare(NLayoutCandidate o1, NLayoutCandidate o2) {
                List<Integer> position1 = getFilterPositionSet(o1);
                List<Integer> position2 = getFilterPositionSet(o2);
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

            private List<Integer> getFilterPositionSet(final NLayoutCandidate candidate) {
                List<Integer> positions = Lists.newArrayList();
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
        String project = candidate.getCuboidLayout().getModel().getProject();
        return ColumnStats.filterColComparator(config, project, null);
    }
}
