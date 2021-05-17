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

package io.kyligence.kap.clickhouse.management;

import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.ClickHouseConfig;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ClickHouseConfigLoader {
    private static final AtomicReference<ClickHouseConfigLoader> INSTANCE = new AtomicReference<>();

    private final File configFile;
    private final AtomicReference<Cluster> cluster = new AtomicReference<>();

    private ClickHouseConfigLoader(File configFile) {
        this.configFile = configFile;
    }

    public static ClickHouseConfigLoader getInstance() {
        if (INSTANCE.get() == null) {
            synchronized (ClickHouseConfigLoader.class) {
                if (INSTANCE.get() == null) {
                    File configFile = new File(ClickHouseConfig.getInstanceFromEnv().getClusterConfig());
                    ClickHouseConfigLoader clickHouseConfigLoader = new ClickHouseConfigLoader(configFile);
                    clickHouseConfigLoader.load();
                    INSTANCE.set(clickHouseConfigLoader);
                }
            }
        }
        return INSTANCE.get();
    }

    public static void clean() {
        synchronized (ClickHouseConfigLoader.class) {
            INSTANCE.set(null);
        }
    }

    public static Yaml getConfigYaml() {
        Constructor constructor = new Constructor(Cluster.class);
        val clusterDesc = new TypeDescription(Cluster.class);
        clusterDesc.addPropertyParameters("nodes", Node.class);
        clusterDesc.addPropertyParameters("socketTimeout", String.class);
        clusterDesc.addPropertyParameters("keepAliveTimeout", String.class);
        clusterDesc.addPropertyParameters("installPath", String.class);
        clusterDesc.addPropertyParameters("logPath", String.class);
        constructor.addTypeDescription(clusterDesc);
        val nodeDesc = new TypeDescription(Node.class);
        nodeDesc.addPropertyParameters("name", String.class);
        nodeDesc.addPropertyParameters("ip", String.class);
        nodeDesc.addPropertyParameters("port", Integer.class);
        nodeDesc.addPropertyParameters("user", String.class);
        nodeDesc.addPropertyParameters("password", String.class);
        constructor.addTypeDescription(nodeDesc);
        return new Yaml(constructor);
    }

    private void load() {
        Yaml yaml = getConfigYaml();
        try {
            cluster.set(yaml.load(new FileInputStream(configFile)));
        } catch (FileNotFoundException e) {
            log.error("ClickHouse config file {} not found", configFile.getAbsolutePath());
        }
    }

    public void refresh() {
        File config = new File(ClickHouseConfig.getInstanceFromEnv().getClusterConfig());
        ClickHouseConfigLoader clickHouseConfigLoader = new ClickHouseConfigLoader(config);
        clickHouseConfigLoader.load();
        INSTANCE.set(clickHouseConfigLoader);
    }

    public Cluster getCluster() {
        return new Cluster(cluster.get());
    }
}
