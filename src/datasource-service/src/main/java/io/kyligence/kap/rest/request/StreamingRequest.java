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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import lombok.Data;
import org.apache.kylin.metadata.model.TableDesc;

import java.util.ArrayList;
import java.util.List;

@Data
public class StreamingRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("table_desc")
    private TableDesc tableDesc;

    @JsonProperty("kafka_config")
    private KafkaConfig kafkaConfig;

    @JsonProperty("successful")
    private boolean successful;

    @JsonProperty("message")
    private String message;

    // CollectKafkaStats.JSON_MESSAGE /CollectKafkaStats.BINARY_MESSAGE
    @JsonProperty("message_type")
    private String messageType;

    @JsonProperty("fuzzy_key")
    private String fuzzyKey;

    @JsonProperty("cluster_index")
    private int clusterIndex;

    @JsonProperty("messages")
    private List<String> messages = new ArrayList<>();

}
