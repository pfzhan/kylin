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

package io.kyligence.kap.metadata.query;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import java.util.List;

public class AccelerateRatioManager {

    private final String project;
    private final KylinConfig kylinConfig;

    private CachedCrudAssist<AccelerateRatio> crud;

    public static AccelerateRatioManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, AccelerateRatioManager.class);
    }

    // called by reflection
    static AccelerateRatioManager newInstance(KylinConfig config, String project) {
        return new AccelerateRatioManager(config, project);
    }

    private AccelerateRatioManager(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        init();
    }

    private void init() {
        final ResourceStore store = ResourceStore.getKylinMetaStore(this.kylinConfig);
        final String resourceRootPath = "/" + this.project + ResourceStore.ACCELERATE_RATIO_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<AccelerateRatio>(store, resourceRootPath, AccelerateRatio.class) {
            @Override
            protected AccelerateRatio initEntityAfterReload(AccelerateRatio entity, String resourceName) {
                return entity;
            }
        };

        crud.reloadAll();
    }

    public AccelerateRatio get() {
        List<AccelerateRatio> accelerateRatios = crud.listAll();
        if (accelerateRatios.isEmpty()) {
            return null;
        }

        return accelerateRatios.get(0);
    }

    public void increment(int numOfQueryHitIndex, int overallQuerySize) {
        AccelerateRatio ratioToUpdate;
        AccelerateRatio cached = get();
        if (cached == null)
            ratioToUpdate = new AccelerateRatio();
        else
            ratioToUpdate = crud.copyForWrite(cached);

        ratioToUpdate.increase(numOfQueryHitIndex, overallQuerySize);
        crud.save(ratioToUpdate);
    }
}
