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

import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.junit.annotation.JdbcMetadataInfo;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.tool.MaintainModeTool;
import lombok.val;

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class MaintainModeToolTest {

    @Test
    public void testForceToExit() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-off", "--force" });
    }

    @Test
    public void testEnterMaintainMode() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test" });
    }

    @Test
    public void testEnterMaintainModeEpochCheck() {
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, true);

        val globalEpoch = epochManager.getGlobalEpoch();
        int id = 1234;
        globalEpoch.setEpochId(id);

        ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", globalEpoch);

        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id);
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test" });

        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id + 1);

        maintainModeTool.execute(new String[] { "-off", "--force" });
        Assert.assertEquals(epochManager.getGlobalEpoch().getEpochId(), id + 2);
    }

    @Test
    public void testCleanProject() {
        MaintainModeTool maintainModeTool = new MaintainModeTool();
        maintainModeTool.execute(new String[] { "-on", "-reason", "test", "-p", "notExistP1" });

        Assert.assertEquals(getEpochStore().list().size(), 2);
    }

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannot init epoch store!");
        }
    }
}
