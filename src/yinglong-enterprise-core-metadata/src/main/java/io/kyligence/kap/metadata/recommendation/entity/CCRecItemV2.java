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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class CCRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("cc")
    private ComputedColumnDesc cc;

    public int[] genDependIds(NDataModel dataModel) {
        Preconditions.checkArgument(cc != null, "CCRecItemV2 without computed column object.");
        Preconditions.checkArgument(StringUtils.isNotEmpty(cc.getExpression()),
                "Computed column expression cannot be null.");
        val exprIdentifiers = ComputedColumnUtil.ExprIdentifierFinder.getExprIdentifiers(cc.getExpression());
        int[] arr = new int[exprIdentifiers.size()];
        ImmutableBiMap<Integer, TblColRef> effectiveCols = dataModel.getEffectiveCols();
        Map<String, Integer> map = Maps.newHashMap();
        effectiveCols.forEach((k, v) -> map.put(v.getIdentity(), k));
        for (int i = 0; i < exprIdentifiers.size(); i++) {
            String columnName = exprIdentifiers.get(i).getFirst() + "." + exprIdentifiers.get(i).getSecond();
            Integer integer = map.get(columnName);
            Preconditions.checkArgument(integer != null, "Computed column referred to a column not on model.");
            arr[i] = integer;
        }
        return arr;
    }
}
