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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MeasureRef extends RecommendationRef {

    public MeasureRef(MeasureDesc measure, int id, boolean existed) {
        this.setId(id);
        this.setEntity(measure);
        this.setContent(JsonUtil.writeValueAsStringQuietly(measure));
        this.setName(measure.getName());
        this.setExisted(existed);
    }

    public NDataModel.Measure getMeasure() {
        return (NDataModel.Measure) getEntity();
    }

    @Override
    public String getDataType() {
        throw new IllegalStateException("Get datatype of MeasureRef is not allowed, but you can get it by getContent");
    }
}
