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
package io.kyligence.kap.tool.routine;

import org.apache.kylin.rest.constant.Constant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.epoch.EpochManager;

public class RoutineToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void testExecuteRoutine1() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] { "--cleanup" });
        Assert.assertTrue(routineTool.isStorageCleanup());
    }

    @Test
    public void testExecuteRoutine2() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] {});
        Assert.assertFalse(routineTool.isStorageCleanup());
    }

    @Test
    public void testExecuteRoutineWithOptionProjects1() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] {});

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] {}, routineTool.getProjects());

    }

    @Test
    public void testExecuteRoutineWithOptionProjects2() {
        RoutineTool routineTool = new RoutineTool();

        routineTool.execute(new String[] { "--projects=ssb" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb" }, routineTool.getProjects());
    }

    @Test
    public void testExecuteRoutineWithOptionProjects3() {
        RoutineTool routineTool = new RoutineTool();

        routineTool.execute(new String[] { "--projects=ssb,default" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb", "default" }, routineTool.getProjects());
    }

    @Test
    public void testExecuteRoutineReleaseEpochs1() {
        EpochManager epochManager = EpochManager.getInstance(getTestConfig());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);

        RoutineTool routineTool = new RoutineTool();
        Epoch epoch = epochManager.getGlobalEpoch();
        Assert.assertEquals(1, epoch.getMvcc());
        Assert.assertFalse(epochManager.isMaintenanceMode());

        routineTool.execute(new String[] { "--cleanup" });

        epoch = epochManager.getGlobalEpoch();
        Assert.assertTrue(epochManager.isMaintenanceMode());
        Assert.assertEquals(2, epoch.getMvcc());
    }

}
