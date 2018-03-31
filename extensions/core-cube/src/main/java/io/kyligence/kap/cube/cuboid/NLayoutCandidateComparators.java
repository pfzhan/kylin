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
import java.util.SortedSet;

import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class NLayoutCandidateComparators {

    private static final Logger logger = LoggerFactory.getLogger(NLayoutCandidateComparators.class);

    public static Comparator<NLayoutCandidate> simple() {
        return new Comparator<NLayoutCandidate>() {
            @Override
            public int compare(NLayoutCandidate o1, NLayoutCandidate o2) {
                return o2.getCuboidLayout().getOrderedDimensions().size()
                        - o1.getCuboidLayout().getOrderedDimensions().size();
            }
        };
    }

    public static Comparator<NLayoutCandidate> matchQueryPattern(final ImmutableSet<TblColRef> dimensions,
            final ImmutableSet<TblColRef> filters, final ImmutableSet<FunctionDesc> measures) {
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

                for (TblColRef filterCol : filters) {
                    DeriveInfo deriveInfo = candidate.getDerivedToHostMap().get(filterCol);
                    if (deriveInfo == null) {
                        addPosition(candidate, positions, filterCol);
                    } else {
                        TblColRef[] hostCols = deriveInfo.columns;
                        for (TblColRef hostCol : hostCols) {
                            addPosition(candidate, positions, hostCol);
                        }
                    }
                }
                return positions;
            }

            private void addPosition(NLayoutCandidate candidate, SortedSet<Integer> positions, TblColRef filterCol) {
                Integer p1 = candidate.getCuboidLayout().getDimensionPos(filterCol);
                if (p1 == null) {
                    // e.g. kylin-it/src/test/resources/query/sql_subquery/query12.sql
                    logger.info("Filter column " + filterCol + " not found in cuboid layout "
                            + candidate.getCuboidLayout() + "'s dimensions, skip to use it for cuboid comparison");
                    return;
                }
                positions.add(p1);
            }

        };
    }
}
