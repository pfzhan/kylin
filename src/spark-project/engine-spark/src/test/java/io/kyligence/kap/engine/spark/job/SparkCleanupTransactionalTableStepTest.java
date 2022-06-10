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
package io.kyligence.kap.engine.spark.job;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import java.io.IOException;

import io.kyligence.kap.job.execution.step.SparkCleanupTransactionalTableStep;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

public class SparkCleanupTransactionalTableStepTest extends NLocalWithSparkSessionTest {

    @Test
    public void testGenerateDropTableCommand() {
        SparkCleanupTransactionalTableStep sparkCleanupTransactionalTableStep = new SparkCleanupTransactionalTableStep();
        String expectResult = "USE `TEST_CDP`;\nDROP TABLE IF EXISTS `TEST_HIVE_TX_INTERMEDIATE5c5851ef8544`;\n";
        String tableFullName = "TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544";
        String cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);

        expectResult = "DROP TABLE IF EXISTS `TEST_HIVE_TX_INTERMEDIATE5c5851ef8544`;\n";
        tableFullName = "TEST_HIVE_TX_INTERMEDIATE5c5851ef8544";
        cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);

        expectResult = "";
        tableFullName = "";
        cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);
    }

    @Test
    public void testDoWork() {
        SparkCleanupTransactionalTableStep step = new SparkCleanupTransactionalTableStep(0);
        step.setProject("SSB");

        try {
            createHDFSFile();
            step.doWork(null);
        } catch (ExecuteException e) {
            Assert.assertEquals("Can not delete intermediate table", e.getMessage());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void createHDFSFile() throws IOException {
        KylinConfig config = getTestConfig();
        String dir = config.getJobTmpTransactionalTableDir("SSB", null);
        Path path = new Path(dir);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            fileSystem.setPermission(path, new FsPermission((short)00777));
            path = new Path(dir + "/TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544");
            fileSystem.createNewFile(path);
        }
    }
}
