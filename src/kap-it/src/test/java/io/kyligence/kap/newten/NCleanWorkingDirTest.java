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
package io.kyligence.kap.newten;

import java.io.File;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.newten.auto.NAutoTestBase;

public class NCleanWorkingDirTest extends NAutoTestBase {

    @Test
    public void testCleanWorkingDir() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql", 0, 1).execute();

        String hdfsWorkingDirectory = kylinConfig.getHdfsWorkingDirectory();
        File workingDir = Paths.get(hdfsWorkingDirectory).getParent().toFile();

        File[] children = workingDir.listFiles();
        if (children != null) {
            for (File child : children) {
                // MetaStoreUtil and NSparkExecutable could create temp files start with "kylin_job_meta"
                if (child.toString().contains("kylin_job_meta")) {
                    Assert.fail();
                }
            }
        }
    }
}
