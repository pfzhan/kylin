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
package io.kyligence.kap.secondstorage.management.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Data
public class StorageRequest {
    @JsonProperty("project")
    private String project;
    @JsonProperty("model")
    private String model;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("segment_ids")
    private List<String> segmentIds;
    @JsonProperty("segment_names")
    private List<String> segmentNames;
    @JsonProperty("type")
    private StorageType type = StorageType.CLICKHOUSE;

    public List<String> getSegmentIds() {
        return segmentIds == null ? Collections.emptyList(): segmentIds;
    }

    public List<String> getSegmentNames() {
        return segmentNames == null ? Collections.emptyList(): segmentNames;
    }

    public enum StorageType {
        CLICKHOUSE
    }

    private static final Set<StorageType> SUPPORTED_STORAGE =
            Stream.of(StorageType.CLICKHOUSE).collect(Collectors.toSet());

    public boolean storageTypeSupported() {
        return SUPPORTED_STORAGE.contains(type);
    }
}
