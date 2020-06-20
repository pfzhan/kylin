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
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MeasureRef extends RecommendationRef {

    private List<ColumnRef> columnRefs;
    private MeasureDesc measure;

    public MeasureRef(MeasureDesc measure, int id) {
        this.measure = measure;
        this.id = id;
        this.columnRefs = Lists.newArrayList();
    }

    protected MeasureRef(int id) {
        this.id = id;
        this.deleted = true;
    }

    @Override
    public List<ColumnRef> getDependencies() {
        return columnRefs;
    }

    @Override
    public String getContent() {
        return JsonUtil.writeValueAsStringQuietly(measure);
    }

    @Override
    public String getName() {
        return measure.getName();
    }

    public static class DeleteMeasureRef extends MeasureRef {

        public DeleteMeasureRef(int id) {
            super(id);
        }
    }
}
