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
package org.apache.kylin.rest.delegate;

import java.util.List;

import org.apache.kylin.common.util.SpringContext;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.rest.service.ProjectService;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ProjectMetadataInvoker {
    private static ProjectMetadataContract delegate = null;

    public static synchronized void setDelegate(ProjectMetadataContract delegate) {
        if (ProjectMetadataInvoker.delegate != null) {
            log.warn("Delegate has been set as {}", delegate);
        }
        ProjectMetadataInvoker.delegate = delegate;
    }

    private ProjectMetadataContract getDelegate() {
        if (delegate == null) {
            // Generally delegate will be set in ContractConfig, here is used for test
            delegate = SpringContext.getBean(ProjectService.class);
        }
        return delegate;
    }

    public void updateRule(List<FavoriteRule.AbstractCondition> conditions, boolean isEnabled, String ruleName,
                           String project) {
        getDelegate().updateRule(conditions, isEnabled, ruleName, project);
    }

    public void saveAsyncTask(String project, AsyncAccelerationTask task) {
        getDelegate().saveAsyncTask(project, task);
    }

}
