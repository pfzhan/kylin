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

package io.kyligence.kap.rest.service;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.rest.service.BasicService;
import org.apache.spark.sql.SparderFunc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;

@Component("configService")
public class ConfigService extends BasicService {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ConfigService.class);

    private static final String[] cubeLevelKylinExposedKeys = new String[] { //
            "kylin.cube.algorithm", //
            "kylin.cube.aggrgroup.max-combination", //
            "kylin.job.sampling-percentage", //
            "kylin.source.hive.redistribute-flat-table", //
            "kylin.storage.hbase.max-region-count", //
            "kylin.storage.hbase.region-cut-gb", //
            "kylin.storage.hbase.hfile-size-gb", //
            "kylin.storage.hbase.compression-codec", //
            "kylin.engine.mr.reduce-input-mb", //
            "kylin.engine.mr.max-reducer-number", //
            "kylin.engine.mr.mapper-input-rows", //
            "kylin.query.disable-cube-noagg-sql", //
    };

    private static final String[] cubeLevelKAPPlusExposedKeys = new String[] { //
            "kylin.cube.algorithm", //
            "kylin.cube.aggrgroup.max-combination", //
            "kylin.job.sampling-percentage", //
            "kylin.source.hive.redistribute-flat-table", //
            "kylin.engine.mr.reduce-input-mb", //
            "kylin.engine.mr.max-reducer-number", //
            "kylin.engine.mr.mapper-input-rows", //
            "kylin.query.disable-cube-noagg-sql", //
    };

    private static final String[] projectLevelKylinExposedKeys = new String[] { //
            "kylin.query.force-limit", //
    };

    private static final String[] projectLevelKAPPlusExposedKeys = new String[] { //
            "kylin.query.force-limit", //
    };

    public Map<String, String> getDefaultConfigMap(String scopeType) {
        if (isKAPPlusVersion()) {
            return getDefaultKAPPlusConfigMap(scopeType);
        } else {
            return getDefaultKylinConfigMap(scopeType);
        }
    }

    private Map<String, String> getDefaultKAPPlusConfigMap(String scopeType) {
        Properties allKylinProps = getAllKylinProperties();

        Map<String, String> result = Maps.newHashMap();

        String[] keys = cubeLevelKAPPlusExposedKeys;
        if (scopeType.equalsIgnoreCase("cube")) {
            keys = cubeLevelKAPPlusExposedKeys;
        } else if (scopeType.equalsIgnoreCase("project")) {
            keys = projectLevelKAPPlusExposedKeys;
        }

        for (String key : keys) {
            result.put(key, allKylinProps.getProperty(key));
        }

        return result;
    }

    private Map<String, String> getDefaultKylinConfigMap(String scopeType) {
        Properties allKylinProps = getAllKylinProperties();

        Map<String, String> result = Maps.newHashMap();

        String[] keys = cubeLevelKylinExposedKeys;
        if (scopeType.equalsIgnoreCase("cube")) {
            keys = cubeLevelKylinExposedKeys;
        } else if (scopeType.equalsIgnoreCase("project")) {
            keys = projectLevelKylinExposedKeys;
        }

        for (String key : keys) {
            result.put(key, allKylinProps.getProperty(key));
        }

        return result;
    }

    public Properties getAllKylinProperties() {
        // hack to get all config properties
        Properties allProps = null;
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            Method getAllMethod = KylinConfigBase.class.getDeclaredMethod("getAllProperties");
            getAllMethod.setAccessible(true);
            allProps = (Properties) getAllMethod.invoke(kylinConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return allProps;
    }

    public String getSparkDriverConf(String confName) {
        return SparderFunc.getSparkConf(confName);
    }

    private boolean isKAPPlusVersion() {
        return System.getProperty("kap.version") != null && System.getProperty("kap.version").contains("Plus");
    }
}
