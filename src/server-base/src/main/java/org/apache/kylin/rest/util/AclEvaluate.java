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

package org.apache.kylin.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.metadata.project.NProjectManager;

@Component("aclEvaluate")
public class AclEvaluate {
    @Autowired
    private AclUtil aclUtil;

    private ProjectInstance getProjectInstance(String projectName) {
        return NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(projectName);
    }

    public void checkProjectAdminPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        aclUtil.hasProjectAdminPermission(projectInstance);
    }

    //for raw project
    public void checkProjectReadPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        aclUtil.hasProjectReadPermission(projectInstance);
    }

    public void checkProjectWritePermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        aclUtil.hasProjectWritePermission(projectInstance);
    }

    public void checkProjectOperationPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        aclUtil.hasProjectOperationPermission(projectInstance);
    }

    //for cube acl entity
    public void checkProjectReadPermission(NDataflow dataflow) {
        ProjectInstance projectInstance = getProjectInstance(dataflow.getProject());
        aclUtil.hasProjectReadPermission(projectInstance);
    }

    public void checkProjectWritePermission(NDataflow dataflow) {
        ProjectInstance projectInstance = getProjectInstance(dataflow.getProject());
        aclUtil.hasProjectWritePermission(projectInstance);
    }

    public void checkProjectOperationPermission(NDataflow dataflow) {
        ProjectInstance projectInstance = getProjectInstance(dataflow.getProject());
        aclUtil.hasProjectOperationPermission(projectInstance);
    }

    /*
    //for job acl entity
    public void checkProjectReadPermission(JobInstance job) {
        aclUtil.hasProjectReadPermission(getProjectByJob(job));
    }
    
    public void checkProjectWritePermission(JobInstance job) {
        aclUtil.hasProjectWritePermission(getProjectByJob(job));
    }
    
    public void checkProjectOperationPermission(JobInstance job) {
        aclUtil.hasProjectOperationPermission(getProjectByJob(job));
    }
    */
    // ACL util's method, so that you can use AclEvaluate
    public String getCurrentUserName() {
        return aclUtil.getCurrentUserName();
    }

    public boolean hasCubeReadPermission(NDataflow dataflow) {
        ProjectInstance projectInstance = getProjectInstance(dataflow.getProject());
        return hasProjectReadPermission(projectInstance);
    }

    public boolean hasProjectReadPermission(ProjectInstance project) {
        boolean _hasProjectReadPermission = false;
        try {
            _hasProjectReadPermission = aclUtil.hasProjectReadPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectReadPermission;
    }

    public boolean hasProjectOperationPermission(ProjectInstance project) {
        boolean _hasProjectOperationPermission = false;
        try {
            _hasProjectOperationPermission = aclUtil.hasProjectOperationPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectOperationPermission;
    }

    public boolean hasProjectWritePermission(ProjectInstance project) {
        boolean _hasProjectWritePermission = false;
        try {
            _hasProjectWritePermission = aclUtil.hasProjectWritePermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectWritePermission;
    }

    public boolean hasProjectAdminPermission(ProjectInstance project) {
        boolean _hasProjectAdminPermission = false;
        try {
            _hasProjectAdminPermission = aclUtil.hasProjectAdminPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectAdminPermission;
    }

    public void checkIsGlobalAdmin() {
        aclUtil.checkIsGlobalAdmin();
    }

}