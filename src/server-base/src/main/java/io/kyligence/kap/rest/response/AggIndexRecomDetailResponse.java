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

package io.kyligence.kap.rest.response;

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.rest.util.PagingUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@NoArgsConstructor
@Data
public class AggIndexRecomDetailResponse {
    // index id
    @JsonProperty("id")
    private long id;

    @JsonProperty("content")
    private List<String> content = Lists.newArrayList();

    @JsonProperty("size")
    private int size;

    public AggIndexRecomDetailResponse(IndexEntity indexEntity, NDataModel optimizedModel, String content,
            int pageOffset, int pageSize) {
        setId(indexEntity.getId());

        val columnIdMap = optimizedModel.getAllNamedColumns().stream()
                .collect(toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn));
        val measureIdMap = optimizedModel.getAllMeasures().stream()
                .collect(toMap(NDataModel.Measure::getId, MeasureDesc::getName));

        extractContent(
                Stream.concat(indexEntity.getDimensions().stream(), indexEntity.getMeasures().stream())
                        .collect(Collectors.toList()),
                content, Stream.concat(columnIdMap.entrySet().stream(), measureIdMap.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        this.size = this.content.size();
        this.content = PagingUtil.cutPage(this.content, pageOffset, pageSize);
    }

    private void extractContent(List<Integer> source, String content, Map<Integer, String> columnIdMap) {
        source.stream().forEach(columnId -> {
            val name = columnIdMap.get(columnId);
            if (StringUtils.isNotEmpty(content) && !name.contains(content.trim().toUpperCase()))
                return;

            this.content.add(name);
        });
    }
}
