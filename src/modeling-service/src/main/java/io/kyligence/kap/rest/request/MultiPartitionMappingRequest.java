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

package io.kyligence.kap.rest.request;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import io.kyligence.kap.metadata.model.MultiPartitionKeyMappingImpl;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class MultiPartitionMappingRequest implements ProjectInsensitiveRequest {
    @JsonProperty("project")
    private String project;

    @JsonProperty("alias_columns")
    private List<String> aliasCols;

    @JsonProperty("multi_partition_columns")
    private List<String> partitionCols;

    @JsonProperty("value_mapping")
    private List<MappingRequest<List<String>, List<String>>> valueMapping;

    public MultiPartitionKeyMappingImpl convertToMultiPartitionMapping() {
        return new MultiPartitionKeyMappingImpl(partitionCols, aliasCols, mappingsToPairs());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MappingRequest<T1, T2> {
        T1 origin;
        T2 target;
    }

    public List<Pair<List<String>, List<String>>> mappingsToPairs() {
        return this.valueMapping != null ? this.valueMapping.stream()
                .map(mapping -> Pair.newPair(mapping.origin, mapping.target)).collect(Collectors.toList()) : null;
    }

}
