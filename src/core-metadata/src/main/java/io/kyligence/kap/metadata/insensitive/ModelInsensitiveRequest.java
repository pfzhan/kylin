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


package io.kyligence.kap.metadata.insensitive;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import io.kyligence.kap.common.util.Unsafe;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;

public interface ModelInsensitiveRequest extends InsensitiveRequest {

    default List<String> inSensitiveFields() {
        return Collections.singletonList("alias");
    }

    @Slf4j
    final class LogHolder {
    }

    @Override
    default void updateField() {
        for (String fieldName : inSensitiveFields()) {
            Field field = getDeclaredField(this.getClass(), fieldName);
            if (field == null) {
                return;
            }
            Unsafe.changeAccessibleObject(field, true);
            try {
                String modelAlias = (String) field.get(this);
                if (StringUtils.isEmpty(modelAlias)) {
                    continue;
                }
                // get project filed from object
                Field projectField = getDeclaredField(this.getClass(), "project");

                if (projectField == null) {
                    continue;
                }
                Unsafe.changeAccessibleObject(projectField, true);
                String projectName = (String) projectField.get(this);

                if (StringUtils.isEmpty(projectName)) {
                    continue;
                }

                NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                if (projectInstance == null) {
                    continue;
                }

                projectName = projectInstance.getName();

                projectField.set(this, projectName);

                modelAlias = getDataModelAlias(modelAlias, projectName);

                field.set(this, modelAlias);
            } catch (IllegalAccessException e) {
                LogHolder.log.warn("update model name failed ", e);
            }
        }
    }

    default String getDataModelAlias(String modelAlias, String projectName) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (dataModel != null) {
            return dataModel.getAlias();
        }
        return modelAlias;
    }
}
