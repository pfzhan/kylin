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

import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.FilterColEnum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

/**
 * Used for select the best-cost candidate for query or auto-modeling
 */
public class ComparatorUtils {

    private ComparatorUtils() {
    }

    public static Comparator<NLayoutCandidate> simple() {
        return (o1, o2) -> o2.getLayoutEntity().getOrderedDimensions().size()
                - o1.getLayoutEntity().getOrderedDimensions().size();
    }

    /**
     * Return comparator for non-filter column
     */
    public static Comparator<TblColRef> nonFilterColComparator() {
        return (col1, col2) -> {
            Preconditions.checkArgument(col1 != null && col1.getFilterLevel() == FilterColEnum.NONE);
            Preconditions.checkArgument(col2 != null && col2.getFilterLevel() == FilterColEnum.NONE);
            return col1.getIdentity().compareToIgnoreCase(col2.getIdentity());
        };
    }

    /**
     * Return comparator for filter column
     */
    public static Comparator<TblColRef> filterColComparator(ChooserContext chooserContext) {
        return Ordering.from(filterLevelComparator()).compound(cardinalityComparator(chooserContext));
    }

    /**
     * cannot deal with null col, if need compare null cols, plz add another comparator,
     * for example, @see nullLastComparator
     *
     * @return
     */
    private static Comparator<TblColRef> filterLevelComparator() {
        return (col1, col2) -> {
            // priority desc
            if (col1 != null && col2 != null) {
                return col2.getFilterLevel().getPriority() - col1.getFilterLevel().getPriority();
            }
            return 0;
        };
    }

    public static Comparator<TblColRef> cardinalityComparator(ChooserContext chooserContext) {
        return (col1, col2) -> {
            if (col1 == null || col2 == null)
                return 0;

            final ColumnStats ret1 = chooserContext.getColumnStats(col1);
            final ColumnStats ret2 = chooserContext.getColumnStats(col2);
            //null last
            if (ret2 == null && ret1 == null) {
                return col1.getIdentity().compareToIgnoreCase(col2.getIdentity());
            } else if (ret2 == null) {
                return -1;
            } else if (ret1 == null) {
                return 1;
            }
            // getCardinality desc
            return Long.compare(ret2.getCardinality(), ret1.getCardinality());
        };
    }

    public static <T> Comparator<T> nullLastComparator() {
        return (t1, t2) -> {
            if (t1 == null && t2 != null) {
                return 1;
            } else if (t2 == null && t1 != null) {
                return -1;
            }
            return 0;
        };
    }
}
