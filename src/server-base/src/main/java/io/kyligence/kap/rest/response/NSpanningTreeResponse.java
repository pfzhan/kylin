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

package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Setter
@Getter
public class NSpanningTreeResponse {
    @JsonProperty("nodes")
    private final Map<Long, TreeNodeResponse> nodesMap = Maps.newHashMap();
    @JsonProperty("roots")
    private final List<TreeNodeResponse> roots = Lists.newArrayList();

    @Getter
    @Setter
    public static class TreeNodeResponse {

        @JsonProperty("cuboid")
        private SimplifiedCuboidResponse cuboid;
        @JsonProperty("children")
        private List<Long> children = Lists.newLinkedList();
        @JsonProperty("parent")
        private long parent = -1;
        @JsonProperty("level")
        private int level;
    }

    @Getter
    @Setter
    public static class SimplifiedCuboidResponse {
        @JsonProperty("id")
        private long id;
        @JsonProperty("status")
        private CuboidStatus status = CuboidStatus.AVAILABLE;
        @JsonProperty("storage_size")
        private long storageSize;
    }

}
