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

package io.kyligence.kap.smart.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.query.util.DefaultQueryTransformer;
import org.apache.kylin.query.util.KeywordDefaultDirtyHack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.util.CognosParenthesesEscape;
import io.kyligence.kap.query.util.EscapeTransformer;
import io.kyligence.kap.smart.query.mockup.MockupPushDownRunner;
import io.kyligence.kap.smart.query.mockup.MockupStorage;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private Utils() { }

    public static KylinConfig newKylinConfig(String metadataUrl) {
        Properties props;
        try {
            props = KylinConfig.getInstanceFromEnv().exportToProperties();
        } catch (Exception e) {
            logger.warn("Pass KylinConfig export exception", e);
            props = new Properties();
        }
        setLargeCuboidCombinationConf(props);
        props.setProperty("kylin.storage.provider.0", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.1", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.2", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.20", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.99", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.100", MockupStorage.class.getName());
        props.setProperty("kylin.env", "DEV");
        props.setProperty("kylin.metadata.url", metadataUrl);
        props.setProperty("kylin.cube.aggrgroup.is-mandatory-only-valid", "true");
        props.setProperty("kylin.metadata.data-model-impl", "io.kyligence.kap.metadata.model.KapModel");

        List<String> queryTransformers = Lists.newArrayList();
        queryTransformers.add(EscapeTransformer.class.getName());
        queryTransformers.add(DefaultQueryTransformer.class.getName());
        queryTransformers.add(KeywordDefaultDirtyHack.class.getName());
        queryTransformers.add(CognosParenthesesEscape.class.getName());
        props.setProperty("kylin.query.transformers", StringUtils.join(queryTransformers, ","));

        return KylinConfig.createKylinConfig(props);
    }

    public static KylinConfig smartKylinConfig(String metadataUrl) {
        Properties props;
        try {
            props = KylinConfig.getInstanceFromEnv().exportToProperties();
        } catch (Exception e) {
            logger.warn("Pass KylinConfig export exception", e);
            props = new Properties();
        }
        setLargeCuboidCombinationConf(props);
        props.setProperty("kylin.env", "DEV");
        props.setProperty("kylin.metadata.url", metadataUrl);
        props.setProperty("kylin.metadata.data-model-impl", "io.kyligence.kap.metadata.model.NDataModel");
        props.setProperty("kylin.metadata.data-model-manager-impl",
                "io.kyligence.kap.metadata.model.NDataModelManager");
        props.setProperty("kylin.metadata.project-manager-impl", "io.kyligence.kap.metadata.project.NProjectManager");
        props.setProperty("kylin.metadata.realization-providers", "io.kyligence.kap.cube.model.NDataflowManager");
        props.setProperty("kylin.query.schema-factory", "io.kyligence.kap.query.schema.KapSchemaFactory");

        List<String> queryTransformers = Lists.newArrayList();
        queryTransformers.add(EscapeTransformer.class.getName());
        queryTransformers.add(DefaultQueryTransformer.class.getName());
        queryTransformers.add(KeywordDefaultDirtyHack.class.getName());
        queryTransformers.add(CognosParenthesesEscape.class.getName());
        props.setProperty("kylin.query.transformers", StringUtils.join(queryTransformers, ","));

        return KylinConfig.createKylinConfig(props);
    }

    public static void setLargeCuboidCombinationConf(Properties props) {
        props.setProperty("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void setLargeCuboidCombinationConf(Map<String, String> props) {
        props.put("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void removeCuboidCombinationConf(Map<String, String> props) {
        props.remove("kylin.cube.aggrgroup.max-combination");
    }

    public static void setLargeCuboidCombinationConf(KylinConfig props) {
        props.setProperty("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void setLargeRowkeySizeConf(KylinConfig kylinConfig) {
        kylinConfig.setProperty("kylin.cube.rowkey.max-size", Integer.toString(Integer.MAX_VALUE - 1));
    }

    public static void setLargeRowkeySizeConf(Map<String, String> props) {
        props.put("kylin.cube.rowkey.max-size", Integer.toString(Integer.MAX_VALUE - 1));
    }

    public static void removeLargeRowkeySizeConf(Map<String, String> props) {
        props.remove("kylin.cube.aggrgroup.max-combination");
    }

    public static void clearCacheForKylinConfig(KylinConfig kylinConfig) throws IOException {
        Broadcaster.getInstance(kylinConfig).notifyNonStaticListener(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE,
                Broadcaster.SYNC_ALL);
    }

    public static void exposeAllTableAndColumn(KylinConfig kylinConfig) {
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name", MockupPushDownRunner.class.getName());
    }
}
