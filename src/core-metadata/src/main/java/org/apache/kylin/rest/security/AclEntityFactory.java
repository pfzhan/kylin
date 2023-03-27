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

package org.apache.kylin.rest.security;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.base.Preconditions;

/**
 * @author xduo
 */
public class AclEntityFactory {

    private AclEntityFactory() {
    }

    public static RootPersistentEntity createAclEntity(String entityType, String uuid) {
        Preconditions.checkNotNull(entityType);
        switch (entityType) {
        case AclEntityType.N_DATA_MODEL:
            NDataModel modelInstance = new NDataModel();
            modelInstance.setUuid(uuid);
            return modelInstance;
        case AclEntityType.PROJECT_INSTANCE:
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setUuid(uuid);
            return projectInstance;
        default:
            throw new IllegalArgumentException("Unsupported entity type " + entityType);
        }
    }
}
