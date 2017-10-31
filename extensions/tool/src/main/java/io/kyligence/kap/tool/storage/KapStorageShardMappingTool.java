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

package io.kyligence.kap.tool.storage;

import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;

import io.kyligence.kap.storage.parquet.steps.ColumnarStorageUtils;
import io.kyligence.kap.storage.parquet.steps.ParquetCubeInfoCollectionStep;

public class KapStorageShardMappingTool {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.exit(1);
        }
        String cubeName = args[0];

        Long cuboidId = null;
        if (args.length > 1) {
            cuboidId = Long.valueOf(args[1]);
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        for (CubeSegment segment : cube.getSegments()) {
            String segmentInfo = ColumnarStorageUtils.getWriteSegmentDir(cube, segment) + ParquetCubeInfoCollectionStep.CUBE_INFO_NAME;

            FileSystem fs = HadoopUtil.getFileSystem(segmentInfo);
            ObjectInputStream ois = new ObjectInputStream(fs.open(new Path(segmentInfo)));
            Map<Long, Set<String>> cuboidMapping = (Map<Long, Set<String>>) ois.readObject();
            System.out.println("dump segment " + segment);
            dump(segment, cuboidMapping, cuboidId);
        }
    }

    private static void dump(CubeSegment segment, Map<Long, Set<String>> cuboidMapping, Long target) {
        System.out.println("cuboid id -> file number -> shard number");
        for (Long cuboid : cuboidMapping.keySet()) {
            if (target == null) {
                System.out.println(String.format("%d -> %d -> %d", cuboid, cuboidMapping.get(cuboid).size(),
                        segment.getCuboidShardNum(cuboid)));
            } else if (target.equals(cuboid)) {
                System.out.println(String.format("%d -> %d -> %d", cuboid, cuboidMapping.get(cuboid).size(),
                        segment.getCuboidShardNum(cuboid)));
            }
        }
    }
}
