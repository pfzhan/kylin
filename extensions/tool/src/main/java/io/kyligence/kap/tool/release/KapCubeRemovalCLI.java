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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;

public class KapCubeRemovalCLI {

    private void removeCube(KylinConfig config, String cubeName) throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.getCube(cubeName);
        if (cube != null) {
            RawTableManager rawTableManager = RawTableManager.getInstance(config);
            RawTableInstance raw = rawTableManager.getRawTableInstance(cubeName);
            if (raw != null) {
                rawTableManager.dropRawTableInstance(cubeName, true);
            }
            ExecutableManager executableManager = ExecutableManager.getInstance(config);
            for (AbstractExecutable job : executableManager.getAllExecutables()) {
                if (CubingExecutableUtil.getCubeName(job.getParams()).equals(cubeName)) {
                    executableManager.deleteJob(job.getId());
                }
            }
            cubeManager.dropCube(cubeName, true);
        }
    }

    public static void main(String[] args) throws IOException {

        KapCubeRemovalCLI cli = new KapCubeRemovalCLI();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        for (String cubeName : args) {
            cli.removeCube(config, cubeName);
        }
    }

}
