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

package io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * currently only check deadline
 * 
 * TODO: check kylin.storage.partition.max-scan-bytes
 */
public class ResourceTrackingGTScanner implements IGTScanner {
    private static final Logger logger = LoggerFactory.getLogger(ResourceTrackingGTScanner.class);

    final private IGTScanner delegate;
    final private long deadline;

    private long rowCount = 0;
    private long rowBytes = 0;

    public ResourceTrackingGTScanner(IGTScanner delegate, long timeout) {
        this.delegate = delegate;
        this.deadline = System.currentTimeMillis() + timeout;
        logger.info("Local deadline is: " + deadline);
    }

    @Override
    public GTInfo getInfo() {
        return delegate.getInfo();
    }

    @Override
    public long getScannedRowCount() {
        return delegate.getScannedRowCount();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        final Iterator<GTRecord> iterator = delegate.iterator();

        return new Iterator<GTRecord>() {
            @Override
            public boolean hasNext() {
                if ((rowCount % GTScanRequest.terminateCheckInterval == 1) && System.currentTimeMillis() > deadline) {
                    throw new KylinTimeoutException("scan timeout happened for this partition");
                }

                return iterator.hasNext();
            }

            @Override
            public GTRecord next() {
                rowCount++;
                return iterator.next();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }
}
