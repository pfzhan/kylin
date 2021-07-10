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

package io.kyligence.kap.metadata.streaming;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@Data
public class KafkaConfig extends RootPersistentEntity implements Serializable {
    @JsonProperty("database")
    private String database;

    @JsonProperty("name")
    private String name;

    @JsonProperty("project")
    private String project;

    @JsonProperty("kafka_bootstrap_servers")
    private String kafkaBootstrapServers;

    @JsonProperty("subscribe")
    private String subscribe;

    @JsonProperty("starting_offsets")
    private String startingOffsets;

    @JsonProperty("batch_table_identity")
    private String batchTable;

    @JsonProperty("parser_name")
    private String parserName;

    private Map<String, String> kafkaParam;

    public KafkaConfig() {
    }

    public KafkaConfig(KafkaConfig other) {
        this.uuid = other.uuid;
        this.lastModified = other.lastModified;
        this.createTime = other.createTime;
        this.name = other.name;
        this.kafkaBootstrapServers = other.kafkaBootstrapServers;
        this.subscribe = other.subscribe;
        this.startingOffsets = other.startingOffsets;
        this.project = other.project;
        this.parserName = other.parserName;
        this.batchTable = other.batchTable;
    }

    public Map<String, String> getKafkaParam() {
        Preconditions.checkState(
                this.kafkaBootstrapServers != null && this.subscribe != null && this.startingOffsets != null,
                "table are not streaming table");
        if (kafkaParam == null) {
            kafkaParam = Maps.<String, String> newHashMap();
            kafkaParam.put("kafka.bootstrap.servers", this.kafkaBootstrapServers);
            kafkaParam.put("subscribe", subscribe);
            kafkaParam.put("startingOffsets", startingOffsets);
            kafkaParam.put("failOnDataLoss", "false");
        }
        return kafkaParam;
    }

    public String getIdentity() {
        return resourceName();
    }

    @Override
    public String resourceName() {
        String originIdentity = String.format(Locale.ROOT, "%s.%s", this.database, this.name);
        return originIdentity.toUpperCase(Locale.ROOT);
    }

    public String getResourcePath() {
        return concatResourcePath(resourceName(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.KAFKA_RESOURCE_ROOT).append("/")
                .append(name).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getBatchTableAlias() {
        return this.batchTable.split("\\.")[1];
    }

    public boolean hasBatchTable() {
        return StringUtils.isNotEmpty(this.batchTable);
    }
}
