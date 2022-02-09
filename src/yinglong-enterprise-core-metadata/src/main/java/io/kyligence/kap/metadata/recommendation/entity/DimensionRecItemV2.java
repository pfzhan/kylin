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

package io.kyligence.kap.metadata.recommendation.entity;

import java.io.Serializable;
import java.util.Map;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DimensionRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("column")
    private NDataModel.NamedColumn column;
    @JsonProperty("data_type")
    private String dataType;

    public int[] genDependIds(Map<String, RawRecItem> uniqueRecItemMap, String content, NDataModel dataModel) {
        if (uniqueRecItemMap.containsKey(content)) {
            return new int[] { -1 * uniqueRecItemMap.get(content).getId() };
        } else {
            String[] arr = content.split(RawRecUtil.TABLE_COLUMN_SEPARATOR);
            if (arr.length == 2) {
                try {
                    Map<String, TableRef> tableAliasMap = dataModel.getAliasMap();
                    Preconditions.checkArgument(tableAliasMap.containsKey(arr[0]));
                    TableRef tableRef = tableAliasMap.get(arr[0]);
                    ColumnDesc tableColumn = RawRecUtil.findColumn(arr[1], tableRef.getTableDesc());
                    String columnName = column.getAliasDotColumn().split("\\.")[1];
                    Preconditions.checkArgument(tableColumn.getName().equalsIgnoreCase(columnName));
                } catch (Exception e) {
                    log.error("validate DimensionRecItemV2 dependIds error.", e);
                    return new int[] { Integer.MAX_VALUE };
                }
            }
            return new int[] { getColumn().getId() };
        }
    }
}
