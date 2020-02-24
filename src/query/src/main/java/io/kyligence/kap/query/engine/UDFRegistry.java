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

package io.kyligence.kap.query.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.measure.MeasureTypeFactory;

/**
 * Registry for all UDFs.
 * Currently it just works as a wrapper for udf registered in kylinConfig and MeasureTypeFactory
 * TODO: move udf registration here
 */
public class UDFRegistry {

    private KylinConfig kylinConfig;
    private Map<String, UDFDefinition> udfDefinitions = new HashMap<>();

    public static UDFRegistry getInstance(KylinConfig config, String project) {
        return config.getManager(project, UDFRegistry.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static UDFRegistry newInstance(KylinConfig conf, String project) {
        try {
            String cls = UDFRegistry.class.getName();
            Class<? extends UDFRegistry> clz = ClassUtil.forName(cls, UDFRegistry.class);
            return clz.getConstructor(KylinConfig.class, String.class).newInstance(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    public UDFRegistry(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;

        //udf
        for (Map.Entry<String, String> entry : KylinConfig.getInstanceFromEnv().getUDFs().entrySet()) {
            udfDefinitions.put(entry.getKey(), new UDFDefinition(entry.getKey().trim().toUpperCase(), entry.getValue().trim()));
        }
        //udaf
        for (Map.Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udfDefinitions.put(entry.getKey(), new UDFDefinition(entry.getKey().trim().toUpperCase(), entry.getValue().getName().trim(), null));
        }
    }

    public Collection<UDFDefinition> getUdfDefinitions() {
        return Collections.unmodifiableCollection(udfDefinitions.values());
    }

    public static class UDFDefinition {
        String name;
        String className;
        String methodName;
        List<String> paths = null;

        public UDFDefinition(String name, String className) {
            this(name, className, "*");
        }

        public UDFDefinition(String name, String className, String methodName) {
            this.name = name;
            this.className = className;
            this.methodName = methodName;
        }

        public String getName() {
            return name;
        }

        public String getClassName() {
            return className;
        }

        public String getMethodName() {
            return methodName;
        }

        public List<String> getPaths() {
            return paths;
        }
    }
}

