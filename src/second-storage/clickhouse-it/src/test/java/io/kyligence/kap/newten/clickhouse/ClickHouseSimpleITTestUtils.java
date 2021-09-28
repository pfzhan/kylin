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
package io.kyligence.kap.newten.clickhouse;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Optional;

public class ClickHouseSimpleITTestUtils {

    public static void checkLockOperateResult(EnvelopeResponse response, List<String> expectedLocks, String project) {
        Assertions.assertEquals(response.getCode(), "000");
        Optional<Manager<NodeGroup>> nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project);
        List<NodeGroup> nodeGroups = nodeGroupManager.get().listAll();
        nodeGroups.stream().forEach(x -> {
            Assertions.assertIterableEquals(x.getLockTypes(), expectedLocks);
        });
    }
}
