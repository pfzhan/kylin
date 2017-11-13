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

package io.kyligence.kap;

import io.kyligence.kap.engine.mr.common.KapBatchConstants;
import io.kyligence.kap.storage.parquet.steps.ParquetSpliceTarballMapper;
import io.kyligence.kap.storage.parquet.steps.ParquetTarballMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.query.KylinTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;

public class ITKapParquetReplicationTest extends KylinTestBase {

    private static Mapper.Context context;
    private static byte[] valueByte = ("Hello, world").getBytes();
    private static Path inputPath = new Path("hdfs:///kylin/kystorage/parquet_replication_IT_test.testid");
    private static Path outputPath = null;
    private static Path spliceOutputPath = null;

    private static Configuration conf  = new Configuration();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        context = Mockito.mock(Mapper.Context.class, new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                throw new RuntimeException(invocationOnMock.getMethod().getName() + " is not stubbed");
            }
        });

        conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        Mockito.doReturn(conf).when(context).getConfiguration();

        Mockito.doReturn(new TaskAttemptID("TaskAttemptID", 124, true, 123, 132)).when(context).getTaskAttemptID();

        conf.set("mapreduce.fileoutputcommitter.algorithm.version", "1");
        Mockito.doReturn(new FileOutputCommitter(inputPath, context)).when(context).getOutputCommitter();

        FileSystem fs = FileSystem.get(inputPath.toUri(), context.getConfiguration());
        if (fs.exists(inputPath.getParent()))
            fs.delete(inputPath, true);
        fs.create(new Path(inputPath, "tmp"));

        FileSplit fileSplit = new FileSplit(inputPath, 0L, 1024L, null);
        Mockito.doReturn(fileSplit).when(context).getInputSplit();

        String cuboidId = inputPath.getParent().getName();
        String shardId = inputPath.getName().substring(0, inputPath.getName().indexOf('.'));
        if (!fs.exists(new Path(inputPath.getParent(), shardId + ".parquet.inv")))
            fs.create(new Path(inputPath.getParent(), shardId + ".parquet.inv"));
        outputPath = new Path(FileOutputFormat.getWorkOutputPath(context), cuboidId + "/" + shardId + ".parquettar");
        spliceOutputPath = new Path(FileOutputFormat.getWorkOutputPath(context), shardId + ".parquettar");
    }

    @Test
    public void testParquetTarballMapper() throws Exception {
        FileSystem fileSystem = FileSystem.get(outputPath.toUri(), context.getConfiguration());
        ParquetTarballMapper tarballMapper = new ParquetTarballMapper();

        context.getConfiguration().set(KapBatchConstants.KYLIN_COLUMNAR_DFS_REPLICATION, "3");
        tarballMapper.doSetup(context);
        tarballMapper.doMap(new IntWritable(1), valueByte, context);
        tarballMapper.doCleanup(context);

        assertEquals(fileSystem.getFileStatus(outputPath).getReplication(), (short)3);

        context.getConfiguration().set(KapBatchConstants.KYLIN_COLUMNAR_DFS_REPLICATION, "2");
        tarballMapper.doSetup(context);
        tarballMapper.doMap(new IntWritable(2), valueByte, context);
        tarballMapper.doCleanup(context);

        assertEquals(fileSystem.getFileStatus(outputPath).getReplication(), (short)2);

    }

    @Test
    public void testParquetSpliceTarballMapper() throws Exception {
        FileSystem fileSystem = FileSystem.get(spliceOutputPath.toUri(), context.getConfiguration());
        ParquetSpliceTarballMapper tarballMapper = new ParquetSpliceTarballMapper();

        context.getConfiguration().set(KapBatchConstants.KYLIN_COLUMNAR_DFS_REPLICATION, "3");
        tarballMapper.doSetup(context);
        tarballMapper.doMap(new IntWritable(1), valueByte, context);
        tarballMapper.doCleanup(context);

        assertEquals(fileSystem.getFileStatus(spliceOutputPath).getReplication(), (short)3);

        context.getConfiguration().set(KapBatchConstants.KYLIN_COLUMNAR_DFS_REPLICATION, "2");
        tarballMapper.doSetup(context);
        tarballMapper.doMap(new IntWritable(2), valueByte, context);
        tarballMapper.doCleanup(context);

        assertEquals(fileSystem.getFileStatus(spliceOutputPath).getReplication(), (short)2);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileSystem fs = HadoopUtil.getFileSystem(inputPath, context.getConfiguration());
        fs.deleteOnExit(inputPath.getParent());
    }
}
