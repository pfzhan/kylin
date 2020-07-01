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

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;

import java.util.List;

@Getter
public class ModelColumnRef extends ColumnRef {

    private final NDataModel.NamedColumn column;
    private String dataType;

    public ModelColumnRef(NDataModel.NamedColumn column, String dataType) {
        this.column = column;
        this.dataType = dataType;
        this.id = column.getId();
        this.existed = true;
        init();
    }

    public void init() {

    }

    @Override
    public String getDataType() {
        return dataType;
    }

    @Override
    public List<RecommendationRef> getDependencies() {
        return Lists.newArrayList();
    }

    @Override
    public String getContent() {
        return getName();
    }

    @Override
    public String getName() {
        return column.getAliasDotColumn();
    }
}
