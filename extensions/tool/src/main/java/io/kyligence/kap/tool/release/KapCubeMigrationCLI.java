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

package io.kyligence.kap.tool.release;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.tool.CubeMigrationCLI;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

public class KapCubeMigrationCLI extends CubeMigrationCLI {

    @Override
    protected void renameFoldersInHdfs(CubeInstance cube) throws IOException {
        if (cube.getDescriptor().getEngineType() == IStorageAware.ID_SHARDED_HBASE)
            super.renameFoldersInHdfs(cube);
        else {
            renameKAPRealizationStoragePath(cube.getUuid());
            RawTableInstance raw = detectRawTable(cube);
            if (null != raw) {
                renameKAPRealizationStoragePath(raw.getUuid());
            }
        }
    }

    @Override
    protected void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot)
            throws IOException {
        super.listCubeRelatedResources(cube, metaResource, dictAndSnapshot);

        RawTableInstance raw = detectRawTable(cube);
        if (null != raw) {
            RawTableDesc desc = raw.getRawTableDesc();
            metaResource.add(raw.getResourcePath());
            metaResource.add(desc.getResourcePath());
        }
    }

    private void renameKAPRealizationStoragePath(String uuid) throws IOException {
        String src = KapConfig.wrap(srcConfig).getParquetStoragePath() + uuid;
        String tgt = KapConfig.wrap(dstConfig).getParquetStoragePath() + uuid;
        Path tgtParent = new Path(KapConfig.wrap(dstConfig).getParquetStoragePath());
        FileSystem fs = FileSystem.get(HadoopUtil.getCurrentConfiguration());
        if (!fs.exists(tgtParent)) {
            fs.mkdirs(tgtParent);
        }
        addOpt(OptType.RENAME_FOLDER_IN_HDFS, new Object[] { src, tgt });
    }

    private RawTableInstance detectRawTable(CubeInstance cube) {
        RawTableInstance rawInstance = RawTableManager.getInstance(srcConfig).getAccompanyRawTable(cube);
        return rawInstance;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        KapCubeMigrationCLI cli = new KapCubeMigrationCLI();
        if (args.length != 8) {
            cli.usage();
            System.exit(1);
        }
        cli.moveCube(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
    }
}
