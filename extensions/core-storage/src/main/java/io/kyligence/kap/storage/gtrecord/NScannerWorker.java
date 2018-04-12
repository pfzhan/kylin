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

package io.kyligence.kap.storage.gtrecord;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.kylin.gridtable.EmptyGTScanner;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NCuboidLayout;

public class NScannerWorker {
    private static final Logger logger = LoggerFactory.getLogger(NScannerWorker.class);
    private IGTScanner internal = null;

    public NScannerWorker(ISegment segment, NCuboidLayout cuboid, GTScanRequest scanRequest, String gtStorage,
            StorageContext context) {
        if (scanRequest == null) {
            logger.info("Segment {} will be skipped", segment);
            internal = new EmptyGTScanner();
            return;
        }

        final GTInfo info = scanRequest.getInfo();

        try {
            IGTStorage rpc = (IGTStorage) Class.forName(gtStorage)
                    .getConstructor(ISegment.class, NCuboidLayout.class, GTInfo.class, StorageContext.class)
                    .newInstance(segment, cuboid, info, context); // default behavior
            internal = rpc.getGTScanner(scanRequest);
        } catch (IOException | InstantiationException | InvocationTargetException | IllegalAccessException
                | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isSegmentSkipped() {
        return internal instanceof EmptyGTScanner;
    }

    public Iterator<GTRecord> iterator() {
        return internal.iterator();
    }

    public void close() throws IOException {
        internal.close();
    }
}
