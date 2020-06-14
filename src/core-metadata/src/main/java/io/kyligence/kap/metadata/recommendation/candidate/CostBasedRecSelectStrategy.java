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

import org.apache.kylin.common.KylinConfig;

public class CostBasedRecSelectStrategy implements RecSelectStrategy {
    private KylinConfig config;

    public CostBasedRecSelectStrategy(KylinConfig config) {
        this.config = config;
    }

    @Override
    public List<RawRecItem> getBestRecItem(int topn) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public List<RawRecItem> getBestRecItemByModel(int topn, String project, String model, int semanticVersion) {
        RawRecommendationManager rawRecommendationManager = RawRecommendationManager.getInstance(config, project);
        return rawRecommendationManager.getCandidatesByModelAndBenefit(project, model, topn);
    }

    @Override
    public List<RawRecItem> getBestRecItemByProject(int topn, String project) {
        RawRecommendationManager rawRecommendationManager = RawRecommendationManager.getInstance(config, project);
        return rawRecommendationManager.getCandidatesByProjectAndBenefit(project, topn);
    }

    public void update(RawRecItem recItem, long time) {
        double latencyOfTheDay = recItem.getLayoutMetric().getLatencyMap().getLatencyByDate(time);
        recItem.setCost((recItem.getCost() + latencyOfTheDay) / Math.E);
    }
}
