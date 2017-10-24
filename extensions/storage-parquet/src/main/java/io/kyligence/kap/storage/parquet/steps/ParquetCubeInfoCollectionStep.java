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

import static io.kyligence.kap.storage.parquet.format.ParquetCubeSpliceOutputFormat.ParquetCubeSpliceWriter.getCuboididFromDiv;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.storage.parquet.format.file.ParquetSpliceReader;
import io.kyligence.kap.storage.parquet.format.file.Utils;

public class ParquetCubeInfoCollectionStep extends AbstractExecutable {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetCubeInfoCollectionStep.class);
    public static final String INPUT_PATH = "input_path";
    public static final String OUTPUT_PATH = "output_path";
    public static final String CUBE_INFO_NAME = "CUBE_INFO";
    public static final String SKIP = "skip";
    public final String WORKING_DIR; // working dir without schema
    private static final String SCHEMA_HINT = "://";

    public ParquetCubeInfoCollectionStep() {
        this(null);
    }

    public ParquetCubeInfoCollectionStep(CubeSegment segment) {
        String fullPath = KapConfig.wrap(segment == null ? KylinConfig.getInstanceFromEnv() : segment.getConfig())
                .getWriteHdfsWorkingDirectory();
        int index = fullPath.indexOf(SCHEMA_HINT);
        if (index >= 0) {
            WORKING_DIR = fullPath.substring(index + SCHEMA_HINT.length());
        } else {
            WORKING_DIR = fullPath;
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        if (getParam(SKIP) != null) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED);
        }

        String inputPath = getParam(INPUT_PATH);
        String outputPath = getParam(OUTPUT_PATH);
        Map<Long, Set<String>> cuboid2FileMap = Maps.newHashMap();

        /**
         * Map cuboid --> file path
         */
        try {
            FileSystem fs = HadoopUtil.getFileSystem(inputPath);
            FileStatus fileStatus = fs.getFileStatus(new Path(inputPath));
            ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path(outputPath), true));
            if (fileStatus.isDirectory()) {
                for (FileStatus child : fs.listStatus(new Path(inputPath))) {
                    if (isParquetFile(child.getPath())) {
                        ParquetSpliceReader reader = new ParquetSpliceReader.Builder().setConf(HadoopUtil.getCurrentConfiguration()).setPath(child.getPath()).setColumnsBitmap(Utils.createBitset(1)).build();
                        for (String div : reader.getDivs()) {
                            long cuboid = getCuboididFromDiv(div);
                            if (!cuboid2FileMap.containsKey(cuboid)) {
                                cuboid2FileMap.put(cuboid, Sets.<String> newHashSet());
                            }

                            String absolutePath = child.getPath().toString();
                            cuboid2FileMap.get(cuboid).add(getRelativePath(absolutePath));
                        }
                    }
                }
            } else {
                logger.error("InputPath {} is not directory", inputPath);
                return new ExecuteResult(ExecuteResult.State.FAILED);
            }

            oos.writeObject(cuboid2FileMap);
            oos.close();
        } catch (IOException e) {
            logger.error("", e);
            return new ExecuteResult(ExecuteResult.State.FAILED);
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED);
    }

    private boolean isParquetFile(Path path) {
        return path.getName().endsWith(".parquettar");
    }

    private String getRelativePath(String absolute) {
        int index = absolute.indexOf(WORKING_DIR);

        if (index < 0) {
            throw new RuntimeException(String.format("File path %s doesn't contain working dir %s", absolute, WORKING_DIR));
        }

        logger.debug("absolute path {}, relative path {}", absolute, absolute.substring(index + WORKING_DIR.length()));
        return absolute.substring(index + WORKING_DIR.length());
    }
}
