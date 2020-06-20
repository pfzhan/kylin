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

package io.kyligence.kap.metadata.recommendation.v2;

import java.util.List;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LayoutRef extends RecommendationRef {
    private List<DimensionRef> dimensionRefs;
    private List<MeasureRef> measureRefs;
    private boolean agg;
    protected LayoutEntity layout;

    public LayoutRef(LayoutEntity layout, int id, boolean agg) {
        this.layout = layout;
        this.dimensionRefs = Lists.newArrayList();
        this.measureRefs = Lists.newArrayList();
        this.id = id;
        this.agg = agg;
    }

    private LayoutRef() {

    }

    @Override
    public List<RecommendationRef> getDependencies() {
        List<RecommendationRef> res = Lists.newArrayList();
        res.addAll(dimensionRefs);
        res.addAll(measureRefs);
        return res;
    }

    @Override
    public String getContent() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    public static class DeleteLayoutRef extends LayoutRef {
        public DeleteLayoutRef(int id) {
            this.id = id;
            this.deleted = true;
        }
    }
}
