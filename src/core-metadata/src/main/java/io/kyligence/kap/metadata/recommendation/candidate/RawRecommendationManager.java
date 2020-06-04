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

package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;

public class RawRecommendationManager {
    private final String project;
    private final KylinConfig kylinConfig;
    private final JdbcRawRecStore jdbcRawRecStore;
    private Map<String, RawRecItem> favoriteQueryMap;

    // CONSTRUCTOR
    public static RawRecommendationManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, RawRecommendationManager.class);
    }

    // called by reflection
    static RawRecommendationManager newInstance(KylinConfig config, String project) throws Exception {
        return new RawRecommendationManager(config, project);
    }

    private RawRecommendationManager(KylinConfig config, String project) throws Exception {
        this.project = project;
        this.kylinConfig = config;
        this.jdbcRawRecStore = new JdbcRawRecStore(config);
        init();
    }

    private void init() {
        loadRawRecItem();
    }

    private void loadRawRecItem() {

    }

    // CURD
    public List<RawRecItem> listAll() {
        return null;
    }

    public List<RawRecItem> getCandidatesByModelAndBenifit(String project, String model, int limit) {
        return null;
    }

    public List<RawRecItem> getCandidatesByProjectAndBenifit(String project, int limit) {
        return null;
    }

    public void save(RawRecItem rawRecItem) {
        jdbcRawRecStore.save(rawRecItem);
    }

    public void batchSave(List<RawRecItem> rawRecItem) {
        jdbcRawRecStore.batchSave();
    }

    public void updateAllCost(String project) {

    }
}
