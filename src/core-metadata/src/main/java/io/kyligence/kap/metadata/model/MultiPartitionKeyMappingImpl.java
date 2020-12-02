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

package io.kyligence.kap.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.Collection;
import java.util.List;

@Getter
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class MultiPartitionKeyMappingImpl extends MultiPartitionKeyMapping {

    private transient List<TblColRef> aliasColumnRefs;
    private transient ImmutableMultimap<List<String>, List<String>> immutableValueMapping;

    public MultiPartitionKeyMappingImpl(List<String> multiPartitionCols, List<String> aliasCols, List<Pair<List<String>, List<String>>> valueMapping) {
        setAliasCols(aliasCols);
        setMultiPartitionCols(multiPartitionCols);
        setValueMapping(valueMapping);
    }

    public void init(NDataModel model) {
        if (CollectionUtils.isEmpty(getAliasCols()) || CollectionUtils.isEmpty(getMultiPartitionCols())) {
            return;
        }

        aliasColumnRefs = Lists.newArrayList();
        for (String columnName : getAliasCols()) {
            aliasColumnRefs.add(model.findColumn(columnName));
        }

        ImmutableMultimap.Builder<List<String>, List<String>> builder = ImmutableMultimap.builder();
        getValueMapping().forEach(pair -> {
            val partitionValue = pair.getFirst();
            val aliasValue = pair.getSecond();

            builder.put(partitionValue, aliasValue);
        });
        immutableValueMapping = builder.build();
    }

    public List<TblColRef> getAliasColumns() {
        return aliasColumnRefs;
    }

    public Collection<List<String>> getAliasValue(List<String> partitionValues) {
        return immutableValueMapping.get(partitionValues);
    }
}
