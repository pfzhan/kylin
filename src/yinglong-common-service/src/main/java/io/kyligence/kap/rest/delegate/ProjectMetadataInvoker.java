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
package io.kyligence.kap.rest.delegate;

import java.util.List;

import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.service.ProjectService;
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
