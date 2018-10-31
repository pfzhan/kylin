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
import com.google.common.collect.Table;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.AbstractColumnAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.ColumnCardinalityAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.ColumnDataTypeQualityAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.ColumnSchemaAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.NumericalColumnAnalysisDelegate;
import io.kyligence.kap.engine.spark.stats.analyzer.delegate.StringColumnAnalysisDelegate;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Row;

public class TableAnalyzer {

    private TableDesc tableDesc;

    private final ColumnDesc[] columns;

    private long rowCounter;

    /**
     * <AbstractColumnAnalysisDelegate.class, colIdx, AbstractColumnAnalysisDelegate.instance>
     */
    private final Table<Class<?>, Integer, AbstractColumnAnalysisDelegate> colDelegateTable;

    public TableAnalyzer(TableDesc tableDesc) {
        this.tableDesc = tableDesc;
        this.columns = tableDesc.getColumns();

        this.colDelegateTable = HashBasedTable.create();

        for (int colIdx = 0; colIdx < this.columns.length; colIdx++) {
            final ColumnDesc columnDesc = columns[colIdx];
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            this.colDelegateTable.put(StringColumnAnalysisDelegate.class, colIdx,
                    new StringColumnAnalysisDelegate(columnDesc));
            this.colDelegateTable.put(ColumnCardinalityAnalysisDelegate.class, colIdx,
                    new ColumnCardinalityAnalysisDelegate(columnDesc));
            this.colDelegateTable.put(ColumnDataTypeQualityAnalysisDelegate.class, colIdx,
                    new ColumnDataTypeQualityAnalysisDelegate(columnDesc));

            if (columnDesc.getType().isNumberFamily()) {
                this.colDelegateTable.put(NumericalColumnAnalysisDelegate.class, colIdx,
                        new NumericalColumnAnalysisDelegate(columnDesc));
            }

            if (ISourceAware.ID_HIVE == tableDesc.getSourceType()) {
                this.colDelegateTable.put(ColumnSchemaAnalysisDelegate.class, colIdx,
                        new ColumnSchemaAnalysisDelegate(columnDesc));
            }
        }

    }

    public void analyze(Row row) {
        rowCounter++;

        for (int colIdx = 0; colIdx < columns.length; colIdx++) {
            if (columns[colIdx].isComputedColumn()) {
                continue;
            }
            final String colValue = row.getString(colIdx);

            for (final Class delegateClass : colDelegateTable.rowKeySet()) {
                final AbstractColumnAnalysisDelegate delegate = colDelegateTable.get(delegateClass, colIdx);
                if (delegate != null) {
                    delegate.analyze(row, colValue);
                }
            }
        }

    }

    public TableAnalyzerResult getResult() {
        return new TableAnalyzerResult(tableDesc, rowCounter, colDelegateTable);
    }
}
