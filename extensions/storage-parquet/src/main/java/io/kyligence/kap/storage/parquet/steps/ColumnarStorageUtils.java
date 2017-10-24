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

package io.kyligence.kap.storage.parquet.steps;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;

public class ColumnarStorageUtils {
    public static String getWriteSegmentDir(RawTableInstance raw, RawTableSegment segment) {
        KapConfig kapConfig = KapConfig.wrap(raw.getConfig());
        return new StringBuffer(kapConfig.getWriteParquetStoragePath()).append(raw.getUuid()).append("/")
                .append(segment.getUuid()).append("/").toString();
    }

    public static String getWriteSegmentDir(CubeInstance cube, CubeSegment segment) {
        KapConfig kapConfig = KapConfig.wrap(cube.getConfig());
        return new StringBuffer(kapConfig.getWriteParquetStoragePath()).append(cube.getUuid()).append("/")
                .append(segment.getUuid()).append("/").toString();
    }

    public static String getReadSegmentDir(CubeSegment segment) {
        return getReadCubeDir(segment.getCubeInstance()) + segment.getUuid() + "/";
    }

    public static String getReadSegmentDir(RawTableSegment segment) {
        return getReadRawtableDir(segment.getRawTableInstance()) + segment.getUuid() + "/";
    }

    public static String getReadRawtableDir(RawTableInstance raw) {
        return new StringBuffer(getLocalParquetStoragePath(raw.getConfig())).append(raw.getUuid()).append("/")
                .toString();
    }

    public static String getReadCubeDir(CubeInstance cube) {
        return new StringBuffer(getLocalParquetStoragePath(cube.getConfig())).append(cube.getUuid()).append("/")
                .toString();
    }

    public static String getLocalParquetStoragePath(KylinConfig config) {
        return KapConfig.wrap(config).getReadParquetStoragePath();
    }
}
