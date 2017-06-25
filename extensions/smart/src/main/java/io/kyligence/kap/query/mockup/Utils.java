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

import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.draft.DraftManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.storage.hybrid.HybridManager;

import io.kyligence.kap.modeling.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

public class Utils {
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

    public static void clearCacheForKylinConfig(KylinConfig kylinConfig) {
        BadQueryHistoryManager.clearCache(kylinConfig);
        Broadcaster.clearCache(kylinConfig);
        CubeDescManager.clearCache(kylinConfig);
        CubeManager.clearCache(kylinConfig);
        CubeOptimizeLogManager.clearCache(kylinConfig);
        DictionaryManager.clearCache(kylinConfig);
        DraftManager.clearCache(kylinConfig);
        ExecutableManager.clearCache(kylinConfig);
        HybridManager.clearCache(kylinConfig);
        MetadataManager.clearCache(kylinConfig);
        ModelStatsManager.clearCache(kylinConfig);
        ProjectManager.clearCache(kylinConfig);
        RealizationRegistry.clearCache(kylinConfig);
    }
}
