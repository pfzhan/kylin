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
package io.kyligence.kap.metadata.project;

import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Collectors;

import io.kyligence.kap.common.persistence.transaction.TransactionLock;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import lombok.val;

public class UnitOfAllWorks {

    public static <T> T doInTransaction(UnitOfWork.Callback<T> f, boolean readonly) {
        return UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.<T>builder().readonly(readonly).unitName(UnitOfWork.GLOBAL_UNIT).processor(
            () -> {
                val projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                val projects = projectManager.listAllProjects().stream()
                        .sorted(Comparator.comparing(RootPersistentEntity::getUuid)).collect(Collectors.toList());
                for (ProjectInstance project : projects) {
                    TransactionLock.getLock(project.getName(), readonly).lock();
                }
                try {
                    return f.process();
                } finally {
                    Collections.reverse(projects);
                    for (ProjectInstance project : projects) {
                        TransactionLock.getLock(project.getName(), readonly).unlock();
                    }
                }
            }
        ).build());
    }

}
