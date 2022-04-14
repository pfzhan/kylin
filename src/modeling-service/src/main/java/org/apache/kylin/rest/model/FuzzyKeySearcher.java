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
package org.apache.kylin.rest.model;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.ref.OptRecV2;
import io.kyligence.kap.metadata.recommendation.ref.RecommendationRef;
import io.kyligence.kap.metadata.recommendation.util.RawRecUtil;

public class FuzzyKeySearcher {

    private FuzzyKeySearcher() {
    }

    public static Set<String> searchComputedColumns(NDataModel model, String key) {

        return model.getComputedColumnDescs().stream()
                .filter(cc -> StringUtils.containsIgnoreCase(cc.getFullName(), key)
                        || StringUtils.containsIgnoreCase(cc.getInnerExpression(), key))
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toSet());
    }

    public static Set<Integer> searchDimensions(NDataModel model, Set<String> ccNameSet, String key) {
        return model.getAllNamedColumns().stream() //
                .filter(NDataModel.NamedColumn::isExist) //
                .filter(c -> StringUtils.containsIgnoreCase(c.getAliasDotColumn(), key)
                        || StringUtils.containsIgnoreCase(c.getName(), key)
                        || ccNameSet.contains(c.getAliasDotColumn()))
                .map(NDataModel.NamedColumn::getId) //
                .collect(Collectors.toSet());
    }

    public static Set<Integer> searchMeasures(NDataModel model, Set<String> ccNameSet, String key) {
        return model.getAllMeasures().stream() //
                .filter(m -> !m.isTomb())
                .filter(m -> StringUtils.containsIgnoreCase(m.getName(), key)
                        || m.getFunction().getParameters().stream()
                                .anyMatch(p -> p.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN)
                                        && (StringUtils.containsIgnoreCase(p.getValue(), key)
                                                || ccNameSet.contains(p.getValue()))))
                .map(NDataModel.Measure::getId).collect(Collectors.toSet());
    }

    public static Set<Integer> searchColumnRefs(OptRecV2 recommendation, Set<String> ccFullNameSet, String key) {
        Set<Integer> recIds = Sets.newHashSet();
        recommendation.getColumnRefs().forEach((id, colRef) -> {
            if (StringUtils.containsIgnoreCase(colRef.getName(), key) //
                    || String.valueOf(colRef.getId()).equals(key) //
                    || ccFullNameSet.contains(colRef.getName())) {
                recIds.add(id);
            }
        });
        return recIds;
    }

    public static Set<Integer> searchCCRecRefs(OptRecV2 recommendation, String key) {
        Set<Integer> recIds = Sets.newHashSet();
        recommendation.getCcRefs().forEach((id, ref) -> {
            RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
            ComputedColumnDesc cc = RawRecUtil.getCC(rawRecItem);
            if (StringUtils.containsIgnoreCase(cc.getFullName(), key)
                    || StringUtils.containsIgnoreCase(cc.getInnerExpression(), key)) {
                recIds.add(id);
                return;
            }
            for (int dependID : rawRecItem.getDependIDs()) {
                if (StringUtils.equalsIgnoreCase(String.valueOf(dependID), key)) {
                    recIds.add(id);
                    return;
                }
            }
        });
        return recIds;
    }

    public static Set<Integer> searchDependRefIds(OptRecV2 recommendation, Set<Integer> dependRefIds, String key) {
        Map<Integer, RecommendationRef> map = Maps.newHashMap();
        recommendation.getDimensionRefs().forEach(map::putIfAbsent);
        recommendation.getMeasureRefs().forEach(map::putIfAbsent);
        Set<Integer> recIds = Sets.newHashSet();
        map.forEach((id, ref) -> {
            if (StringUtils.containsIgnoreCase(ref.getName(), key)
                    || (ref.getId() >= 0 && String.valueOf(ref.getId()).equals(key))) {
                recIds.add(id);
                return;
            }
            for (RecommendationRef dependency : ref.getDependencies()) {
                if (dependRefIds.contains(dependency.getId())) {
                    recIds.add(id);
                    return;
                }
            }
        });
        return recIds;
    }
}
