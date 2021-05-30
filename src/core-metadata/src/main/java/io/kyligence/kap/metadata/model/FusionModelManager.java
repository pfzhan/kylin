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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.model;

import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FusionModelManager {
    private static final Logger logger = LoggerFactory.getLogger(FusionModelManager.class);

    public static FusionModelManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, FusionModelManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static FusionModelManager newInstance(KylinConfig config, String project) {
        return new FusionModelManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<FusionModel> crud;

    private FusionModelManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.FUSION_MODEL_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<FusionModel>(getStore(), resourceRootPath, FusionModel.class) {
            @Override
            protected FusionModel initEntityAfterReload(FusionModel t, String resourceName) {
                t.init(config, project);
                return t;
            }
        };
        crud.reloadAll();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public FusionModel getFusionModel(String modelId) {
        if (org.apache.commons.lang.StringUtils.isEmpty(modelId)) {
            return null;
        }
        return crud.get(modelId);
    }

    public FusionModel dropModel(String id) {
        val model = getFusionModel(id);
        if (model == null) {
            return null;
        }
        crud.delete(model);
        return model;
    }

    public FusionModel createModel(FusionModel desc) {
        if (desc == null) {
            throw new IllegalArgumentException();
        }
        if (crud.contains(desc.resourceName()))
            throw new IllegalArgumentException("Fusion Model  '" + desc.getAlias() + "' already exists");

        return crud.save(desc);
    }
}
