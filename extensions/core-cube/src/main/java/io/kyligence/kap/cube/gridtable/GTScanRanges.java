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

package io.kyligence.kap.cube.gridtable;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

public class GTScanRanges {
    private TreeSet<ScanRange> rangeTreeSet = Sets.newTreeSet();
    private OptimizeStrategy optimizationClazz = new DefaultOptimization();
    private GTInfo info = null;

    public GTScanRanges() {
        this(Sets.<ScanRange> newTreeSet());
    }

    public GTScanRanges(TreeSet<ScanRange> rangeTreeSet) {
        this.rangeTreeSet = rangeTreeSet;
        if (!rangeTreeSet.isEmpty() && rangeTreeSet.first() != null) {
            this.info = rangeTreeSet.first().startKey.getInfo();
        }
    }

    public GTScanRanges(GTRecord[] starts, GTRecord[] ends, int[] rowCounts) {
        if (starts != null && ends != null && rowCounts != null) {
            Preconditions.checkArgument(starts.length == ends.length);
            Preconditions.checkArgument(starts.length == rowCounts.length);

            for (int i = 0; i < starts.length; i++) {
                Preconditions.checkArgument(starts[i].compareTo(ends[i]) <= 0, "Start key must not be larger than end key.");

                // handle overlap from input
                addScanRage(new ScanRange(starts[i], ends[i], rowCounts[i]));
            }

            if (starts.length > 0 && starts[0] != null) {
                this.info = starts[0].getInfo();
            }
        }
    }

    public TreeSet<ScanRange> getRangeSet() {
        return rangeTreeSet;
    }

    public void addScanRage(ScanRange scanRange) {
        if (rangeTreeSet.isEmpty()) {
            rangeTreeSet.add(scanRange);
        } else {
            ScanRange prevRange = rangeTreeSet.floor(scanRange);
            ScanRange currRange = null;
            if (prevRange != null && prevRange.isConnected(scanRange)) {
                currRange = prevRange;
                currRange.endKey = Ordering.natural().max(prevRange.endKey, scanRange.endKey);
                currRange.rowCount = Ordering.natural().max(prevRange.rowCount, scanRange.rowCount);
            } else {
                currRange = scanRange;
                rangeTreeSet.add(currRange);
            }
            Iterator<ScanRange> tailIter = rangeTreeSet.tailSet(scanRange, false).iterator();
            ScanRange tmpRange = null;
            while (tailIter.hasNext()) {
                tmpRange = tailIter.next();
                if (tmpRange.isConnected(scanRange)) {
                    tailIter.remove();
                    currRange.endKey = Ordering.natural().max(tmpRange.endKey, currRange.endKey);
                    currRange.rowCount = Ordering.natural().max(tmpRange.rowCount, currRange.rowCount);
                }
            }
        }
    }

    public GTScanRanges and(GTScanRanges other) {
        if (other == null || other.rangeTreeSet.isEmpty() || rangeTreeSet.isEmpty()) {
            return new GTScanRanges(Sets.<ScanRange> newTreeSet());
        }

        Iterator<ScanRange> thisScanRangeIter = rangeTreeSet.iterator();
        Iterator<ScanRange> otherScanRangeIter = other.rangeTreeSet.iterator();

        ScanRange currThisScanRange = thisScanRangeIter.next();
        ScanRange currOtherScanRange = otherScanRangeIter.next();
        TreeSet<ScanRange> resultSet = Sets.newTreeSet();
        while (currThisScanRange != null && currOtherScanRange != null) {
            if (ScanRange.isConnected(currThisScanRange, currOtherScanRange)) {
                resultSet.add(ScanRange.intersection(currThisScanRange, currOtherScanRange));
            }

            if (currThisScanRange.endKey.compareTo(currOtherScanRange.endKey) < 0) {
                if (thisScanRangeIter.hasNext()) {
                    currThisScanRange = thisScanRangeIter.next();
                } else {
                    currThisScanRange = null;
                }
            } else {
                if (otherScanRangeIter.hasNext()) {
                    currOtherScanRange = otherScanRangeIter.next();
                } else {
                    currOtherScanRange = null;
                }
            }
        }

        return new GTScanRanges(resultSet);
    }

    public GTScanRanges or(GTScanRanges other) {
        if (other == null) {
            return this;
        }

        TreeSet<ScanRange> resultSet = Sets.newTreeSet();
        if (other.rangeTreeSet.isEmpty() || rangeTreeSet.isEmpty()) {
            resultSet.addAll(other.rangeTreeSet);
            resultSet.addAll(rangeTreeSet);
            return new GTScanRanges(resultSet);
        }

        Iterator<ScanRange> thisScanRangeIter = rangeTreeSet.iterator();
        Iterator<ScanRange> otherScanRangeIter = other.rangeTreeSet.iterator();

        ScanRange currThisScanRange = thisScanRangeIter.next();
        ScanRange currOtherScanRange = otherScanRangeIter.next();
        ScanRange spanRange = null;

        while (currThisScanRange != null && currOtherScanRange != null) {
            if (spanRange != null) {
                if (currThisScanRange.startKey.compareTo(currOtherScanRange.startKey) < 0) {
                    if (spanRange.isConnected(currThisScanRange)) {
                        spanRange = spanRange.span(currThisScanRange);
                        if (thisScanRangeIter.hasNext()) {
                            currThisScanRange = thisScanRangeIter.next();
                        } else {
                            currThisScanRange = null;
                        }
                    } else {
                        resultSet.add(spanRange);
                        spanRange = null;
                    }
                } else {
                    if (spanRange.isConnected(currOtherScanRange)) {
                        spanRange = spanRange.span(currOtherScanRange);
                        if (otherScanRangeIter.hasNext()) {
                            currOtherScanRange = otherScanRangeIter.next();
                        } else {
                            currOtherScanRange = null;
                        }
                    } else {
                        resultSet.add(spanRange);
                        spanRange = null;
                    }
                }
            } else {
                if (ScanRange.isConnected(currThisScanRange, currOtherScanRange)) {
                    spanRange = currThisScanRange.span(currOtherScanRange);
                } else {
                    resultSet.add(currThisScanRange.endKey.compareTo(currOtherScanRange.endKey) < 0 ? currThisScanRange : currOtherScanRange);
                }

                if (currThisScanRange.endKey.compareTo(currOtherScanRange.endKey) < 0) {
                    if (thisScanRangeIter.hasNext()) {
                        currThisScanRange = thisScanRangeIter.next();
                    } else {
                        currThisScanRange = null;
                    }
                } else {
                    if (otherScanRangeIter.hasNext()) {
                        currOtherScanRange = otherScanRangeIter.next();
                    } else {
                        currOtherScanRange = null;
                    }
                }
            }
        }

        while (currThisScanRange != null) {
            if (spanRange != null) {
                if (spanRange.isConnected(currThisScanRange)) {
                    spanRange = spanRange.span(currThisScanRange);
                } else {
                    resultSet.add(spanRange);
                    spanRange = null;
                }
            } else {
                resultSet.add(currThisScanRange);
            }

            if (thisScanRangeIter.hasNext()) {
                currThisScanRange = thisScanRangeIter.next();
            } else {
                currThisScanRange = null;
            }
        }

        while (currOtherScanRange != null) {
            if (spanRange != null) {
                if (spanRange.isConnected(currOtherScanRange)) {
                    spanRange = spanRange.span(currOtherScanRange);
                } else {
                    resultSet.add(spanRange);
                    spanRange = null;
                }
            } else {
                resultSet.add(currOtherScanRange);
            }

            if (otherScanRangeIter.hasNext()) {
                currOtherScanRange = otherScanRangeIter.next();
            } else {
                currOtherScanRange = null;
            }
        }

        if (spanRange != null) {
            resultSet.add(spanRange);
        }

        return new GTScanRanges(resultSet);
    }

    public GTScanRanges not() {
        Preconditions.checkNotNull(info);

        TreeSet<ScanRange> resultSet = Sets.newTreeSet();

        ScanRange prevRange = null;
        ScanRange currRange = new ScanRange(new GTRecord(info), new GTRecord(info), 0);
        for (ScanRange aRange : rangeTreeSet) {
            prevRange = currRange;
            currRange = aRange;
            resultSet.add(new ScanRange(prevRange.endKey, currRange.startKey, 0));
        }
        resultSet.add(new ScanRange(currRange.endKey, new GTRecord(info), 0));

        return new GTScanRanges(resultSet);
    }

    // Not idempotent - as some optimization might be approximate solution
    public GTScanRanges optimize() {
        if (optimizationClazz != null) {
            optimizationClazz.optimize(this);
        }
        return this;
    }

    @Override
    public String toString() {
        if (rangeTreeSet.isEmpty())
            return StringUtils.EMPTY;

        StringBuilder sb = new StringBuilder();
        for (ScanRange range : rangeTreeSet) {
            sb.append(range.toString()).append(",");
        }
        return sb.substring(0, sb.length() - 1);
    }

    private interface OptimizeStrategy {
        void optimize(GTScanRanges scanRanges);
    }

    public static class ScanRange implements Comparable<ScanRange> {
        GTRecord startKey;
        GTRecord endKey;
        int rowCount;

        public ScanRange(GTRecord startKey, GTRecord endKey, int rowCount) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.rowCount = rowCount;
        }

        public static boolean isConnected(ScanRange range1, ScanRange range2) {
            if (range1 == null || range2 == null)
                return false;
            return range1.startKey.compareTo(range2.endKey) <= 0 && range1.endKey.compareTo(range2.startKey) >= 0;
        }

        public static ScanRange intersection(ScanRange range1, ScanRange range2) {
            if (range1 == null || range2 == null)
                return null;
            return new ScanRange(Ordering.natural().max(range1.startKey, range2.startKey), Ordering.natural().min(range1.endKey, range2.endKey), 0);
        }

        public boolean isConnected(ScanRange other) {
            return isConnected(this, other);
        }

        public ScanRange intersection(ScanRange other) {
            return intersection(this, other);
        }

        public ScanRange span(ScanRange other) {
            return new ScanRange(Ordering.natural().min(startKey, other.startKey), Ordering.natural().max(endKey, other.endKey), 0);
        }

        @Override
        public int compareTo(ScanRange other) {
            return startKey.compareTo(other.startKey);
        }

        @Override
        public String toString() {
            return "[" + startKey + "," + endKey + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((startKey == null) ? 0 : startKey.hashCode());
            result = prime * result + ((endKey == null) ? 0 : endKey.hashCode());
            result = prime * result + rowCount;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ScanRange other = (ScanRange) obj;
            if (startKey == null) {
                if (other.startKey != null)
                    return false;
            } else if (!startKey.equals(other.startKey))
                return false;
            if (endKey == null) {
                if (other.endKey != null)
                    return false;
            } else if (!endKey.equals(other.endKey))
                return false;
            return true;
        }
    }

    private class DefaultOptimization implements OptimizeStrategy {

        @Override
        public void optimize(GTScanRanges scanRanges) {

        }
    }

    public List<GTScanRange> getGTRangeList() {
        List<GTScanRange> result = Lists.newArrayListWithCapacity(rangeTreeSet.size());
        for (ScanRange range : rangeTreeSet) {
            result.add(new GTScanRange(range.startKey, range.endKey));
        }
        return result;
    }
}
