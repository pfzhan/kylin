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
import java.util.Map;
import java.util.SortedSet;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.BiMap;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidLayout;

public class NCuboidLayoutComparators {
    public static Comparator<NCuboidLayout> simple() {
        return new Comparator<NCuboidLayout>() {
            @Override
            public int compare(NCuboidLayout o1, NCuboidLayout o2) {
                return o2.getOrderedDimensions().size() - o1.getOrderedDimensions().size();
            }
        };
    }

    public static Comparator<NCuboidLayout> cheapest() {
        return new Comparator<NCuboidLayout>() {
            @Override
            public int compare(NCuboidLayout o1, NCuboidLayout o2) {
                return o1.getOrderedDimensions().size() - o2.getOrderedDimensions().size();
            }
        };
    }

    public static Comparator<NCuboidLayout> scored(final Map<Long, ? extends Number> scores) {
        return new Comparator<NCuboidLayout>() {
            @Override
            public int compare(NCuboidLayout o1, NCuboidLayout o2) {
                double delta = scores.get(o2.getId()).doubleValue() - scores.get(o1.getId()).doubleValue();
                return delta > 0 ? 1 : (delta == 0 ? 0 : -1);
            }
        };
    }

    public static Comparator<NCuboidLayout> matchQueryPattern(final ImmutableBitSet dimensions,
            final ImmutableBitSet filters, final ImmutableBitSet measures) {
        return new Comparator<NCuboidLayout>() {
            private SortedSet<Integer> getFilterPositionSet(final NCuboidLayout layout) {
                SortedSet<Integer> positions = Sets.newTreeSet(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });

                BiMap<Integer, TblColRef> colsMap = layout.getCuboidDesc().getCubePlan().getModel()
                        .getEffectiveColsMap();
                for (int fDimId : filters) {
                    Integer p1 = layout.getDimensionPosMap().get(fDimId);
                    if (p1 != null) {
                        positions.add(p1);
                    } else {
                        DeriveInfo hostInfo = layout.getDeriveInfo(colsMap.get(fDimId));
                        for (TblColRef hostCol : hostInfo.columns)
                            positions.add(layout.getDimensionPosMap().get(layout.getModel().getColId(hostCol)));
                    }
                }
                return positions;
            }

            @Override
            public int compare(NCuboidLayout o1, NCuboidLayout o2) {
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
        };
    }
}
