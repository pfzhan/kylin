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
package org.apache.kylin.metadata.cachesync;

import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.val;

@Builder
@AllArgsConstructor
public class CacheReloadChecker<T extends RootPersistentEntity> {

    private ResourceStore store;

    private CachedCrudAssist<T> crud;

    boolean needReload(String resourceName) {
        val entity = crud.getCache().getIfPresent(resourceName);
        if (entity == null) {
            return true;
        } else {
            return checkDependencies(entity);
        }
    }

    private boolean checkDependencies(RootPersistentEntity entity) {
        val raw = store.getResource(entity.getResourcePath());
        if (raw == null) {
            // if still missing, no need to reload
            return !(entity instanceof MissingRootPersistentEntity);
        }
        if (raw.getMvcc() != entity.getMvcc()) {
            return true;
        }

        Preconditions.checkState(!(entity instanceof MissingRootPersistentEntity));

        val entities = entity.getDependencies();
        if (entities == null) {
            return false;
        }
        for (val depEntity : entities) {
            val depNeedReload = checkDependencies(depEntity);
            if (depNeedReload) {
                return true;
            }
        }
        return false;
    }

}
