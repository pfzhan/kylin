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

package io.kyligence.kap.smart;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import lombok.Getter;

public abstract class AbstractSemiAutoContext extends AbstractContext {

    @Getter
    private Map<NDataModel, OptimizeRecommendation> recommendationMap = Maps.newHashMap();

    protected AbstractSemiAutoContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        super(kylinConfig, project, sqlArray);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return OptimizeRecommendationManager.getInstance(getKylinConfig(), getProject()).applyIndexPlan(modelId);
    }

    @Override
    public void changeModelMainType(NDataModel model) {
        model.setManagementType(ManagementType.MODEL_BASED);
    }

    @Override
    public String getIdentifier() {
        return "Auto-Recommendation";
    }
}
