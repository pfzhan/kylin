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

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;

public class ColumnarStorageUtils {
    public static String getSegmentDir(KylinConfig config, RawTableInstance raw, RawTableSegment segment) {
        KapConfig kapConfig = KapConfig.wrap(config);
        return new StringBuffer(kapConfig.getParquetStoragePath()).append(raw.getUuid()).append("/")
                .append(segment.getUuid()).append("/").toString();
    }

    public static String getSegmentDir(KylinConfig config, CubeInstance cube, CubeSegment segment) {
        KapConfig kapConfig = KapConfig.wrap(config);
        return new StringBuffer(kapConfig.getParquetStoragePath()).append(cube.getUuid()).append("/")
                .append(segment.getUuid()).append("/").toString();
    }

    public static String getLocalRawtableDir(KylinConfig config, String remoteFs, RawTableInstance raw) {
        return new StringBuffer(getLocalParquetStoragePath(config, remoteFs)).append(raw.getUuid()).append("/")
                .toString();
    }

    public static String getLocalCubeDir(KylinConfig config, String remoteFs, CubeInstance cube) {
        return new StringBuffer(getLocalParquetStoragePath(config, remoteFs)).append(cube.getUuid()).append("/")
                .toString();
    }

    private static String getLocalParquetStoragePath(KylinConfig config, String remoteFs) {
        KapConfig kapConfig = KapConfig.wrap(config);
        Path localParquetStoragePath = new Path(kapConfig.getParquetStoragePath());
        Path localParquetStoragePathWithoutSchema = Path.getPathWithoutSchemeAndAuthority(localParquetStoragePath);
        Path remotePath = new Path(remoteFs, localParquetStoragePathWithoutSchema);
        String remotePathStr = remotePath.toString();
        if (remotePathStr.endsWith("/") == false) {
            remotePathStr = remotePathStr + "/";
        }

        return remotePathStr;
    }
}
