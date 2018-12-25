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
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.smart.query.mockup.MockupPushDownRunner;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    private Utils() {
    }

    public static KylinConfig newKylinConfig(String metadataUrl) {
        Properties props;
        try {
            props = KylinConfig.getInstanceFromEnv().exportToProperties();
        } catch (Exception e) {
            logger.warn("Ignoring KylinConfig export exception", e);
            props = new Properties();
        }
        setLargeCuboidCombinationConf(props);
        props.setProperty("kylin.env", "DEV");
        props.setProperty("kylin.metadata.url", metadataUrl);
        props.setProperty("kylin.cube.aggrgroup.is-mandatory-only-valid", "true");
        props.setProperty("kap.metadata.mock.no-dict-store", "true");

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
        //        Broadcaster.getInstance(kylinConfig).notifyNonStaticListener(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE,
        //                Broadcaster.SYNC_ALL);
    }

    public static void exposeAllTableAndColumn(KylinConfig kylinConfig) {
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name", MockupPushDownRunner.class.getName());
    }
}
