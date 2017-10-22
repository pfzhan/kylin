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

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;

public class StorageDuplicateStep extends AbstractExecutable {
    /**
     * In this step, local means read cluster (query), remote means write cluster (build)
     */
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        if (!kapConfig.isParquetSeparateFsEnabled()) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        }

        String fsPrefix = kapConfig.getParquetFileSystem();
        if (Strings.isEmpty(fsPrefix)) {
            throw new IllegalArgumentException("kylin.storage.columnar.file-system missing in kylin.properties");
        }

        CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        CubeSegment cubeSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        String remotePath = ColumnarStorageUtils.getSegmentDir(cube, cubeSegment);
        String localPath = ColumnarStorageUtils.getLocalCubeDir(fsPrefix, cube);
        try {
            logger.info("copy cube files: from {} to {}", remotePath, localPath);
            DistCp distCp = new DistCp(HadoopUtil.getCurrentConfiguration(), new DistCpOptions(Lists.newArrayList(new Path(remotePath)), new Path(localPath)));
            distCp.execute();
        } catch (Exception e) {
            logger.info("{}", org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(e));
            throw new ExecuteException(e);
        }

        RawTableInstance raw = RawTableManager.getInstance(cubeSegment.getConfig()).getAccompanyRawTable(cubeSegment.getCubeInstance());
        if (null != raw) {
            RawTableSegment rawSegment = raw.getSegmentById(cubeSegment.getUuid());
            remotePath = ColumnarStorageUtils.getSegmentDir(raw, rawSegment);
            localPath = ColumnarStorageUtils.getLocalRawtableDir(fsPrefix, raw);
            try {
                logger.info("copy rawtable files: from {} to {}", remotePath, localPath);
                DistCp distCp = new DistCp(HadoopUtil.getCurrentConfiguration(), new DistCpOptions(Lists.newArrayList(new Path(remotePath)), new Path(localPath)));
                distCp.execute();
            } catch (Exception e) {
                logger.info("{}", org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(e));
                throw new ExecuteException(e);
            }
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }
}
