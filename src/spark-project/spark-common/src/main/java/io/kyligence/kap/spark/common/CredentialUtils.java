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
package io.kyligence.kap.spark.common;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class CredentialUtils {
    private static Logger logger = LoggerFactory.getLogger(CredentialUtils.class);

    /**
     * wrap spark session with credential
     * @param ss spark session
     * @param project project name
     */
    public static void wrap(SparkSession ss, String project) {
        if (project != null) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NProjectManager manager = config.getManager(NProjectManager.class);
            ProjectInstance projectInstance = manager.getProject(project);
            if (projectInstance.getSourceType() == ISourceAware.ID_FILE) {
                String type = projectInstance.getOverrideKylinProps().get("kylin.source.credential.type");
                String credentialClass = chooseCredential(type);
                String credentialString = projectInstance.getOverrideKylinProps().get("kylin.source.credential.value");
                try {
                    Class clazz = Class.forName(credentialClass);
                    Object object = clazz.newInstance();
                    Method decode = clazz.getDeclaredMethod("decode", String.class);
                    Object o = decode.invoke(object, credentialString);
                    Method method = clazz.getDeclaredMethod("build", SparkSession.class);
                    method.invoke(o, ss);
                } catch (Exception e) {
                    logger.error("Wrap spark session failed ", e);
                }
            }
        }
    }

    /**
     * wrap spark conf with credential
     * @param sparkConf
     * @param project
     */
    public static void wrap(SparkConf sparkConf, String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NProjectManager manager = config.getManager(NProjectManager.class);
        ProjectInstance projectInstance = manager.getProject(project);
        if (projectInstance.getSourceType() == ISourceAware.ID_FILE) {
            String type = projectInstance.getOverrideKylinProps().get("kylin.source.credential.type");
            String credentialClass = chooseCredential(type);
            String credentialString = projectInstance.getOverrideKylinProps().get("kylin.source.credential.value");
            try {
                Class clazz = Class.forName(credentialClass);
                Object object = clazz.newInstance();
                Method decode = clazz.getDeclaredMethod("decode", String.class);
                Object o = decode.invoke(object, credentialString);
                Method method = clazz.getDeclaredMethod("buildConf", SparkConf.class);
                method.invoke(o, sparkConf);
            } catch (Exception e) {
                logger.error("Wrap spark conf failed ", e);
            }
        }
    }

    private static String chooseCredential(String type) {
        try {
            Class<?> clazz = Class.forName("io.kyligence.kap.source.file.CredentialOperator");
            Method chooseCredential = clazz.getDeclaredMethod("chooseCredentialClassName", String.class);
            return (String) chooseCredential.invoke(null, type);
        } catch (Exception e) {
            logger.error("Choose credential error: ", e);
        }
        return null;
    }
}
