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
package io.kyligence.kap.secondstorage.metadata;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@DataDefinition
public class TablePartition implements Serializable, IKeep {

    @JsonProperty("shard_nodes")
    private final List<String> shardNodes = Lists.newArrayList();
    @JsonProperty("size_in_node")
    private Map<String, Long> sizeInNode;
    @JsonProperty("segment_id")
    private String segmentId;
    @JsonProperty("id")
    private String id;
    @JsonProperty("node_file_map")
    private Map<String, List<SegmentFileStatus>> nodeFileMap;

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Long> getSizeInNode() {
        return sizeInNode== null ? Collections.emptyMap() : sizeInNode;
    }

    public List<String> getShardNodes() {
        return shardNodes;
        //return Collections.unmodifiableList(shardNodes);
    }

    public static final class Builder {
        private List<String> shardNodes;
        private String segmentId;
        private String id;
        private Map<String, Long> sizeInNode;
        private Map<String, List<SegmentFileStatus>> nodeFileMap;

        public Builder setSegmentId(String segmentId) {
            this.segmentId = segmentId;
            return this;
        }
        public Builder setShardNodes(List<String> shardNodes) {
            this.shardNodes = shardNodes;
            return this;
        }

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setNodeFileMap(Map<String, List<SegmentFileStatus>> nodeFileMap) {
            this.nodeFileMap = nodeFileMap;
            return this;
        }

        public Builder setSizeInNode(Map<String, Long> sizeInNode) {
            this.sizeInNode = sizeInNode;
            return this;
        }
        public TablePartition build() {
            TablePartition partition = new TablePartition();
            partition.shardNodes.addAll(shardNodes);
            partition.segmentId = segmentId;
            partition.id = id;
            partition.nodeFileMap = nodeFileMap;
            partition.sizeInNode = sizeInNode;
            return partition;
        }
    }
    public String getSegmentId() {
        return segmentId;
    }

    public String getId() {
        return id;
    }

    public Map<String, List<SegmentFileStatus>> getNodeFileMap() {
        return nodeFileMap == null ? Collections.emptyMap() : nodeFileMap;
    }
}
