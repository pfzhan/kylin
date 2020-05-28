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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.metadata.recommendation.candidate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import io.kyligence.kap.common.persistence.metadata.JdbcDataSource;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;

public class JdbcRawRecStore {

    private static final String RECOMMENDATION_CANDIDATE = "_rec_candidate";

    private static final RowMapper<RawRecItem> RAW_REC_ITEM_ROW_MAPPER = new RowMapper<RawRecItem>() {
        @Override
        public RawRecItem mapRow(ResultSet resultSet, int i) throws SQLException {
            RawRecItem rawRecItem = new RawRecItem();
            // TODO fullfill
            // TODO fullfill
            // TODO fullfill
            return rawRecItem;
        }
    };

    private final String table;
    private JdbcTemplate jdbcTemplate;

    public JdbcRawRecStore(KylinConfig config) throws Exception {
        val url = config.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier() + RECOMMENDATION_CANDIDATE;
        createTableIfNotExist();
    }

    private void createTableIfNotExist() {

    }

    public void save(RawRecItem recItem) {

    }

    public void batchSave() {

    }

    public void update() {

    }

    public void addOrUpdate() {

    }

    public void deleteBySemanticVersion(int semanticVersion) {

    }

    public List<RawRecItem> listAll(String project, String semanticVersion, String model) {
        return null;
    }

    public RawRecItem getItemById(long id) {
        return null;
    }

    public List<RawRecItem> getResourceByModelAndType(String project, String semanticVersion, String model,
            RawRecItem.RawRecType type, RawRecItem.RawRecState state) {
        return null;
    }
}
