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

package io.kyligence.kap.query.mockup;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.query.util.CognosParenthesesEscape;
import io.kyligence.kap.query.util.ConvertToComputedColumn;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static KylinConfig newKylinConfig(String metadataUrl) {
        Properties props = new Properties();
        setLargeCuboidCombinationConf(props);
        props.setProperty("kylin.storage.provider.0", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.1", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.2", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.99", MockupStorage.class.getName());
        props.setProperty("kylin.storage.provider.100", MockupStorage.class.getName());
        props.setProperty("kylin.env", "DEV");
        props.setProperty("kylin.metadata.url", metadataUrl);

        List<String> queryTransformers = Lists.newArrayList();
        queryTransformers.add(ConvertToComputedColumn.class.getName());
        queryTransformers.add(CognosParenthesesEscape.class.getName());
        props.setProperty("kylin.query.transformers", StringUtils.join(queryTransformers, ","));

        return KylinConfig.createKylinConfig(props);
    }

    public static void setLargeCuboidCombinationConf(Properties props) {
        props.setProperty("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void setLargeCuboidCombinationConf(LinkedHashMap<String, String> props) {
        props.put("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void setLargeCuboidCombinationConf(KylinConfig props) {
        props.setProperty("kylin.cube.aggrgroup.max-combination", Long.toString(Long.MAX_VALUE - 1));
    }

    public static void clearCacheForKylinConfig(KylinConfig kylinConfig) throws IOException {
        Broadcaster.getInstance(kylinConfig).notifyNonStaticListener(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE,
                Broadcaster.SYNC_ALL);
    }
}
