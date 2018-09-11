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

package io.kyligence.kap.storage.parquet.gtscanner;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;

public class ParquetBytesGTScanner4Cube extends ParquetBytesGTScanner {
    public ParquetBytesGTScanner4Cube(GTInfo info, Iterator<ByteBuffer> iterator, GTScanRequest scanRequest,
                                      long maxScannedBytes, boolean withDelay) {
        super(info, iterator, scanRequest, maxScannedBytes, scanRequest.getTimeout(), withDelay);
    }

    protected ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest) {
        BitSet bs = new BitSet();

        ImmutableBitSet dimensions = scanRequest.getInfo().getPrimaryKey();
        for (int i = 0; i < dimensions.trueBitCount(); ++i) {
            bs.set(dimensions.trueBitAt(i));
        }

        ImmutableBitSet queriedColumns = scanRequest.getColumns();
        for (int i = 0; i < queriedColumns.trueBitCount(); ++i) {
            bs.set(queriedColumns.trueBitAt(i));
        }
        return new ImmutableBitSet(bs);
    }

    protected ImmutableBitSet[] getParquetCoveredColumnBlocks(GTScanRequest scanRequest) {
        
        ImmutableBitSet selectedColBlocksBitSet = scanRequest.getSelectedColBlocks();
        
        ImmutableBitSet[] selectedColBlocks = new ImmutableBitSet[selectedColBlocksBitSet.trueBitCount()];
        
        for(int i = 0; i < selectedColBlocksBitSet.trueBitCount(); i++) {
            
            selectedColBlocks[i] = scanRequest.getInfo().getColumnBlock(selectedColBlocksBitSet.trueBitAt(i));
            
        }

        return selectedColBlocks;
    }

}
