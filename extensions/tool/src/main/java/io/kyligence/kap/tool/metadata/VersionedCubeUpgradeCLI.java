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

package io.kyligence.kap.tool.metadata;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.metadata.scheduler.SchedulerJobInstance;
import io.kyligence.kap.metadata.scheduler.SchedulerJobManager;
import io.kyligence.kap.modeling.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.vube.VubeInstance;
import io.kyligence.kap.vube.VubeManager;
import io.kyligence.kap.vube.VubeUpdate;

public class VersionedCubeUpgradeCLI {

    private static final String UPGRADE = "UPGRADE";

    private static final String CHECK = "CHECK";

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("Args num error");
        }
        String cmd = args[0].toUpperCase();

        switch (cmd) {
        case UPGRADE:
            upgrade(KylinConfig.getInstanceFromEnv());
            break;
        case CHECK:
            boolean needUpgrade = checkIfNeedUpgrade(KylinConfig.getInstanceFromEnv());
            if (needUpgrade) {
                System.out.println("Found metadata of cube without versions, need upgrade.");
                System.exit(2);
            }
            System.exit(1);
        default:
            throw new IllegalArgumentException("Unrecognized cmd");
        }
    }

    public static boolean checkIfNeedUpgrade(KylinConfig kylinConfig) {
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        VubeManager vubeManager = VubeManager.getInstance(kylinConfig);

        if (cubeManager.listAllCubes().size() > 0 && vubeManager.listVubeInstances().size() == 0) {
            return true;
        } else {
            return false;
        }
    }

    public static void upgrade(KylinConfig kylinConfig) throws IOException {
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        VubeManager vubeManager = VubeManager.getInstance(kylinConfig);
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        RawTableManager rawTableManager = RawTableManager.getInstance(kylinConfig);
        SchedulerJobManager schedulerJobManager = SchedulerJobManager.getInstance(kylinConfig);
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(kylinConfig);

        List<CubeInstance> cubeList = cubeManager.listAllCubes();

        for (CubeInstance cube : cubeList) {
            if (!cube.getName().contains("_version_") && vubeManager.getVubeInstance(cube.getName()) == null) {
                ProjectInstance project = projectManager.getProject(cube.getProject());
                VubeInstance vube = vubeManager.createVube(cube.getName(), cube, project.getName(), "ADMIN");
                RawTableInstance raw = rawTableManager.getRawTableInstance(cube.getName());
                SchedulerJobInstance scheduler = schedulerJobManager.getSchedulerJob(cube.getName());

                VubeUpdate update = new VubeUpdate(vube);

                if (scheduler != null) {
                    scheduler.setRealizationType("vube");
                    schedulerJobManager.updateSchedulerJobInstance(scheduler);
                }

                vubeManager.updateVube(update);
            }
        }
    }
}
