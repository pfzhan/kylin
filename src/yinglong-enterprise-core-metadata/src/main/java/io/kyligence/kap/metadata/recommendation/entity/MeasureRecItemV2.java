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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MeasureRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("measure")
    private NDataModel.Measure measure;
    @JsonProperty("param_order")
    private long[] paramOrder;

    public int[] genDependIds(Map<String, RawRecItem> nonLayoutUniqueFlagRecMap, String content, NDataModel dataModel) {
        Set<TableRef> allTables = dataModel.getAllTableRefs();
        Map<String, TableRef> tableMap = Maps.newHashMap();
        allTables.forEach(tableRef -> tableMap.putIfAbsent(tableRef.getAlias(), tableRef));
        Map<String, NDataModel.NamedColumn> namedColumnMap = getNamedColumnMap(dataModel);
        String[] params = content.split("__");
        int[] dependIDs = new int[params.length - 1];
        for (int i = 1; i < params.length; i++) {
            if (nonLayoutUniqueFlagRecMap.containsKey(params[i])) {
                // means it's a cc
                dependIDs[i - 1] = -1 * nonLayoutUniqueFlagRecMap.get(params[i]).getId();
            } else {

                String[] splits = params[i].split(RawRecUtil.TABLE_COLUMN_SEPARATOR);
                if (splits.length == 2) {
                    try {
                        String alias = splits[0];
                        Preconditions.checkArgument(tableMap.containsKey(alias));
                        TableDesc tableDesc = tableMap.get(alias).getTableDesc();
                        ColumnDesc dependColumn = RawRecUtil.findColumn(splits[1], tableDesc);
                        String aliasDotName = String.format(Locale.ROOT, "%s.%s", alias, dependColumn.getName());
                        dependIDs[i - 1] = namedColumnMap.get(aliasDotName).getId();
                    } catch (IllegalArgumentException e) {
                        dependIDs[i - 1] = Integer.MAX_VALUE;
                    }
                } else {
                    dependIDs[i - 1] = Integer.MAX_VALUE;
                }
            }
        }
        return dependIDs;
    }

    public static Map<String, NDataModel.NamedColumn> getNamedColumnMap(NDataModel dataModel) {
        Map<String, NDataModel.NamedColumn> namedColumnMap = Maps.newHashMap();
        dataModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist).forEach(namedColumn -> {
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            namedColumnMap.putIfAbsent(aliasDotColumn, namedColumn);
        });
        return namedColumnMap;
    }
}
