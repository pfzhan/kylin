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
package io.kyligence.kap.rest.aspect;

import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.security.AclEntityType;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.insensitive.InsensitiveRequest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class InsensitiveNameAspect {

    @Around("@within(org.springframework.stereotype.Controller) || @within(org.springframework.web.bind.annotation.RestController)")
    public Object around(ProceedingJoinPoint pjp) throws Throwable {
        Object[] args = pjp.getArgs();
        try {
            MethodSignature signature = (MethodSignature) pjp.getSignature();
            String[] parameterNames = signature.getParameterNames();
            Class[] parameterTypes = signature.getParameterTypes();
            for (int i = 0; i < parameterNames.length; i++) {
                if (args[i] == null) {
                    continue;
                }
                updateInsensitiveField(args, parameterNames, parameterTypes, i);
            }
        } catch (Exception e) {
            log.warn("update insensitive field failed ", e);
        }
        return pjp.proceed(args);
    }

    private void updateInsensitiveField(Object[] args, String[] parameterNames, Class[] parameterTypes, int i) {
        Object arg = args[i];
        if (parameterTypes[i] == String.class) {
            if (StringUtils.isEmpty((String) args[i])) {
                return;
            }
            switch (parameterNames[i]) {
            case "project":
                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject((String) args[i]);
                if (projectInstance != null) {
                    args[i] = projectInstance.getName();
                }
                break;
            case "username":
                NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
                ManagedUser user = userManager.get((String) args[i]);
                if (user != null) {
                    args[i] = user.getUsername();
                }
                break;
            case "modelAlias":
                args[i] = getDataModelAlias(args, parameterNames, parameterTypes, (String) arg);
                break;
            case "type":
            case "sidType":
            case "entityType":
                args[i] = getCaseInsentiveType((String) arg);
                break;
            default:
                break;
            }

        } else if (arg instanceof InsensitiveRequest) {
            InsensitiveRequest insensitiveRequest = (InsensitiveRequest) arg;
            insensitiveRequest.updateField();
        }
    }

    /**
     * get project filed from parameters
     * @param args
     * @param parameterNames
     * @param parameterTypes
     * @param modelAlias
     * @return
     */
    private String getDataModelAlias(Object[] args, String[] parameterNames, Class[] parameterTypes,
            String modelAlias) {
        String projectName = null;

        for (int i = 0; i < parameterNames.length; i++) {
            if (parameterTypes[i] == String.class && Objects.equals("project", parameterNames[i])) {
                projectName = (String) args[i];
                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                if (projectInstance != null) {
                    projectName = projectInstance.getName();
                }
                break;
            }
        }
        if (StringUtils.isEmpty(projectName)) {
            return modelAlias;
        }
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (dataModel != null) {
            return dataModel.getAlias();
        }
        return modelAlias;
    }

    public static String getCaseInsentiveType(String type) {
        Set<String> originTypes = Sets.newHashSet(
                MetadataConstants.TYPE_USER, MetadataConstants.TYPE_GROUP,
                AclEntityType.N_DATA_MODEL, AclEntityType.PROJECT_INSTANCE);
        return originTypes.stream().filter(originType -> originType.equalsIgnoreCase(type)).findFirst().orElse(type);
    }
}
