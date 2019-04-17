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

package io.kyligence.kap.tool.garbage;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class GarbageCleaner {

    private GarbageCleaner() {
    }

    /**
     * trigger by user
     * @param project
     */
    public static void cleanupMetadataManually(String project) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            if (isProjectAutoMain(project))
                new BrokenModelCleaner().cleanup(project);

            new FavoriteQueryCleaner().cleanup(project);
            new IndexCleaner().cleanup(project);
            new ExecutableCleaner().cleanup(project);
            return 0;
        }, project);
    }

    /**
     * trigger by a scheduler that is scheduled at 12:00 am every day
     * @param project
     */
    public static void cleanupMetadataAtScheduledTime(String project) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            if (isProjectAutoMain(project))
                new BrokenModelCleaner().cleanup(project);

            new IndexCleaner().cleanup(project);
            new ExecutableCleaner().cleanup(project);
            return 0;
        }, project);
    }

    private static boolean isProjectAutoMain(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectInstance = NProjectManager.getInstance(config).getProject(project);

        return MaintainModelType.AUTO_MAINTAIN.equals(projectInstance.getMaintainModelType());
    }
}
