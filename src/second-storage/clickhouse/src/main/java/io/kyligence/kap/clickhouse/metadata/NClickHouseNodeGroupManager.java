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
package io.kyligence.kap.clickhouse.metadata;

import io.kyligence.kap.secondstorage.metadata.NManager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.NODE_GROUP;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.RES_PATH_FMT;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.STORAGE_NAME;

public class NClickHouseNodeGroupManager extends NManager<NodeGroup> {

    private static final Logger logger = LoggerFactory.getLogger(NClickHouseNodeGroupManager.class);

    private NClickHouseNodeGroupManager(KylinConfig cfg, final String project) {
        super(cfg, project);
    }

    // called by reflection
    static NClickHouseNodeGroupManager newInstance(KylinConfig config, String project) {
        return new NClickHouseNodeGroupManager(config, project);
    }

    @Override
    protected NodeGroup newRootEntity(String cubeName) {
        return NodeGroup.builder().build();
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public String name() {
        return "NClickHouseNodeGroupManager";
    }

    @Override
    public String rootPath() {
        return String.format(Locale.ROOT, RES_PATH_FMT, project, STORAGE_NAME, NODE_GROUP);
    }

    @Override
    public Class<NodeGroup> entityType() {
        return NodeGroup.class;
    }

}
