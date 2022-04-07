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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;

import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.project.NProjectManager;

@Component("aclEvaluate")
public class AclEvaluate {
    @Autowired
    private AclUtil aclUtil;

    private ProjectInstance getProjectInstance(String projectName) {
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(projectName)) {
            throw new KylinException(EMPTY_PROJECT_NAME, msg.getEMPTY_PROJECT_NAME());
        }

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(projectName);
        if (prjInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getPROJECT_NOT_FOUND(), projectName));
        }
        return prjInstance;
    }

    //for raw project
    public void checkProjectReadPermission(String projectName) {
        aclUtil.hasProjectReadPermission(getProjectInstance(projectName));
    }

    public void checkProjectOperationPermission(String projectName) {
        aclUtil.hasProjectOperationPermission(getProjectInstance(projectName));
    }

    public void checkProjectWritePermission(String projectName) {
        aclUtil.hasProjectWritePermission(getProjectInstance(projectName));
    }

    public void checkProjectAdminPermission(String projectName) {
        aclUtil.hasProjectAdminPermission(getProjectInstance(projectName));
    }

    // ACL util's method, so that you can use AclEvaluate
    public String getCurrentUserName() {
        return aclUtil.getCurrentUserName();
    }

    public boolean hasProjectReadPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectReadPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectOperationPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectOperationPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectWritePermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectWritePermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectAdminPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectAdminPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public void checkIsGlobalAdmin() {
        aclUtil.checkIsGlobalAdmin();
    }

}