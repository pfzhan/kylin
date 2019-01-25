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

package io.kyligence.kap.engine.spark.stats.analyzer;

import com.google.common.collect.HashBasedTable;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.AbstractColumnAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.ColumnCardinalityAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.ColumnSchemaAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.NumericalColumnAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.StringColumnAnalysisDelegate;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import java.io.Serializable;

public class TableAnalyzerResult implements Serializable {

    private TableDesc tableDesc;

    private ColumnDesc[] columnDescs;

    private long rowCount;

    private HashBasedTable<Class<?>, Integer, AbstractColumnAnalysisDelegate> colDelegateTable;

    public TableAnalyzerResult(TableDesc tableDesc, long rowCount,
            HashBasedTable<Class<?>, Integer, AbstractColumnAnalysisDelegate> colDelegateTable) {

        this.tableDesc = tableDesc;
        this.columnDescs = tableDesc.getColumns();
        this.rowCount = rowCount;
        this.colDelegateTable = colDelegateTable;

        encodeResult();
    }

    public static TableAnalyzerResult reduce(TableAnalyzerResult r1, TableAnalyzerResult r2) {
        r1.decodeResult();
        r2.decodeResult();

        final long rowCount = r1.rowCount + r2.rowCount;

        final HashBasedTable<Class<?>, Integer, AbstractColumnAnalysisDelegate> reduceColDelegate = HashBasedTable
                .create();
        for (int colIdx = 0; colIdx < r1.columnDescs.length; colIdx++) {
            if (r1.columnDescs[colIdx].isComputedColumn()) {
                continue;
            }
            for (final Class colDelegateClass : r1.colDelegateTable.rowKeySet()) {
                final AbstractColumnAnalysisDelegate colDelegate = r1.colDelegateTable.get(colDelegateClass, colIdx);
                if (colDelegate != null) {
                    final AbstractColumnAnalysisDelegate anotherColDelegate = r2.colDelegateTable.get(colDelegateClass,
                            colIdx);
                    reduceColDelegate.put(colDelegateClass, colIdx, colDelegate.reduce(anotherColDelegate));
                }
            }
        }

        return new TableAnalyzerResult(r1.tableDesc, rowCount, reduceColDelegate);
    }

    public void encodeResult() {
        for (int colIdx = 0; colIdx < this.columnDescs.length; colIdx++) {
            if (columnDescs[colIdx].isComputedColumn()) {
                continue;
            }
            for (final Class delegateClass : colDelegateTable.rowKeySet()) {
                final AbstractColumnAnalysisDelegate delegate = colDelegateTable.get(delegateClass, colIdx);
                if (delegate != null) {
                    delegate.encode();
                }
            }
        }
    }

    public void decodeResult() {
        for (int colIdx = 0; colIdx < this.columnDescs.length; colIdx++) {
            if (columnDescs[colIdx].isComputedColumn()) {
                continue;
            }
            for (final Class delegateClass : colDelegateTable.rowKeySet()) {
                final AbstractColumnAnalysisDelegate delegate = colDelegateTable.get(delegateClass, colIdx);
                if (delegate != null) {
                    delegate.decode();
                }
            }
        }
    }

    private StringColumnAnalysisDelegate getStringColumnAnalysisDelegate(final int colIdx) {
        return (StringColumnAnalysisDelegate) this.colDelegateTable.get(StringColumnAnalysisDelegate.class, colIdx);
    }

    private ColumnCardinalityAnalysisDelegate getColumnCardinalityAnalysisDelegate(final int colIdx) {
        return (ColumnCardinalityAnalysisDelegate) this.colDelegateTable.get(ColumnCardinalityAnalysisDelegate.class,
                colIdx);
    }

    private NumericalColumnAnalysisDelegate getNumericalColumnAnalysisDelegate(final int colIdx) {
        return (NumericalColumnAnalysisDelegate) this.colDelegateTable.get(NumericalColumnAnalysisDelegate.class,
                colIdx);
    }

    private ColumnSchemaAnalysisDelegate getColumnSchemaAnalysisDelegate(final int colIdx) {
        return (ColumnSchemaAnalysisDelegate) this.colDelegateTable.get(ColumnSchemaAnalysisDelegate.class, colIdx);
    }

    public TableDesc getTableDesc() {
        return tableDesc;
    }

    public ColumnDesc[] getColumns() {
        return columnDescs;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getNullOrBlankCount(final int colIdx) {
        return getStringColumnAnalysisDelegate(colIdx).getNullOrBlankCount();
    }

    public long getCardinality(final int colIdx) {
        return getColumnCardinalityAnalysisDelegate(colIdx).getCardinality();
    }

    public HLLCounter getHLLC(final int colIdx) {
        return getColumnCardinalityAnalysisDelegate(colIdx).getHllC();
    }

    public byte[] getHLLCBytes(final int colIdx) {
        return getColumnCardinalityAnalysisDelegate(colIdx).getBuffer();
    }

    public int getMaxLength(final int colIdx) {
        return getStringColumnAnalysisDelegate(colIdx).getMaxLength();
    }

    public String getMaxLengthValue(final int colIdx) {
        return getStringColumnAnalysisDelegate(colIdx).getMaxLengthValue();
    }

    public int getMinLength(final int colIdx) {
        return getStringColumnAnalysisDelegate(colIdx).getMinLength();
    }

    public String getMinLengthValue(final int colIdx) {
        return getStringColumnAnalysisDelegate(colIdx).getMinLengthValue();
    }

    public double getMaxNumeral(final int colIdx) {
        return getNumericalColumnAnalysisDelegate(colIdx).getMax();
    }

    public double getMinNumeral(final int colIdx) {
        return getNumericalColumnAnalysisDelegate(colIdx).getMin();
    }

    public boolean isIllegalColumnName(final int colIdx) {
        return getColumnSchemaAnalysisDelegate(colIdx).isIllegal();
    }

}
