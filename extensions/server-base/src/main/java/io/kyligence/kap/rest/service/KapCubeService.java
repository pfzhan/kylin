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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.WeakHashMap;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import io.kyligence.kap.rest.response.ColumnarResponse;

@Component("kapCubeService")
public class KapCubeService extends BasicService {

    private WeakHashMap<String, ColumnarResponse> columnarInfoCache = new WeakHashMap<>();

    public ColumnarResponse getColumnarInfo(String segStoragePath, CubeSegment segment) throws IOException {
        String id = segment.getUuid();
        if (columnarInfoCache.containsKey(id)) {
            return columnarInfoCache.get(id);
        }

        ColumnarResponse columnarResp = new ColumnarResponse();
        columnarResp.setDateRangeStart(segment.getDateRangeStart());
        columnarResp.setDateRangeEnd(segment.getDateRangeEnd());

        FileSystem fs = HadoopUtil.getFileSystem(segStoragePath);
        if (fs.exists(new Path(segStoragePath))) {
            ContentSummary cs = fs.getContentSummary(new Path(segStoragePath));
            columnarResp.setFileCount(cs.getFileCount());
        } else {
            columnarResp.setFileCount(0);
        }
        columnarResp.setStorageSize(segment.getSizeKB() * 1024);
        columnarResp.setSegmentName(segment.getName());
        columnarResp.setSegmentUUID(segment.getUuid());
        columnarResp.setSegmentPath(segStoragePath);

        columnarInfoCache.put(id, columnarResp);
        return columnarResp;
    }
}
