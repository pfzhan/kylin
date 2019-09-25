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
package io.kyligence.kap.common.persistence.transaction;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class UnitOfWorkContext {

    private final String project;

    private KylinConfig.SetAndUnsetThreadLocalConfig localConfig;
    private TransactionLock currentLock = null;

    @Delegate
    private UnitOfWorkParams params;

    List<AfterUnitTask> tasks = Lists.newArrayList();

    public void doAfterUnit(AfterUnitTask task) {
        tasks.add(task);
    }

    void cleanResource() {
        if (localConfig == null) {
            return;
        }

        KylinConfig config = localConfig.get();
        ResourceStore.clearCache(config);
        localConfig.close();
        localConfig = null;
    }

    void checkLockStatus() {
        Preconditions.checkNotNull(currentLock);
        Preconditions.checkState(currentLock.isHeldByCurrentThread());

    }

    void checkReentrant(UnitOfWorkParams params) {
        Preconditions.checkState(project.equals(params.getUnitName()) || this.params.isAll(),
                "re-entry of UnitOfWork with different unit name? existing: %s, new: %s", project,
                params.getUnitName());
        Preconditions.checkState(params.isReadonly() == isReadonly(),
                "re-entry of UnitOfWork with different lock type? existing: %s, new: %s", isReadonly(),
                params.isReadonly());
        Preconditions.checkState(params.isUseSandbox() == isUseSandbox(),
                "re-entry of UnitOfWork with different sandbox? existing: %s, new: %s", isReadonly(),
                params.isUseSandbox());
    }

    void runTasks() {
        tasks.forEach(task -> {
            try {
                task.run();
            } catch (Exception e) {
                log.warn("Failed to run task after unit", e);
            }
        });
    }

    public interface AfterUnitTask {
        void run() throws Exception;
    }
}
