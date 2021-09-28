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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@DataDefinition
public class NodeGroup extends RootPersistentEntity implements Serializable, IKeep,
        IManagerAware<NodeGroup> {
    @JsonProperty("nodeNames")
    private List<String> nodeNames = new ArrayList<>();
    @JsonProperty("lock_types")
    private List<String> lockTypes = new ArrayList<>();
    private transient Manager<NodeGroup> manager;

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void setManager(Manager<NodeGroup> manager) {
        this.manager = manager;
    }

    @Override
    public void verify() {
        // node name can't duplicate
        if (nodeNames != null) {
            Preconditions.checkArgument(nodeNames.stream().distinct().count() == nodeNames.size());
        }
        if (lockTypes != null) {
            Preconditions.checkArgument(lockTypes.stream().distinct().count() == lockTypes.size());
        }
    }

    public List<String> getNodeNames() {
        return CollectionUtils.isEmpty(nodeNames) ? Collections.emptyList() : Collections.unmodifiableList(nodeNames);
    }

    public NodeGroup setNodeNames(List<String> nodeNames) {
        this.checkIsNotCachedAndShared();
        this.nodeNames = nodeNames;
        return this;
    }

    public List<String> getLockTypes() {
        return CollectionUtils.isEmpty(lockTypes) ? Collections.emptyList() : Collections.unmodifiableList(lockTypes);
    }

    public NodeGroup setLockTypes(List<String> lockTypes) {
        this.lockTypes = lockTypes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        NodeGroup nodeGroup = (NodeGroup) o;

        return nodeNames != null ? nodeNames.equals(nodeGroup.nodeNames) : nodeGroup.nodeNames == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (nodeNames != null ? nodeNames.hashCode() : 0);
        return result;
    }

    public NodeGroup update(Consumer<NodeGroup> updater) {
        Preconditions.checkArgument(manager != null);
        return manager.update(uuid, updater);
    }

    public static final class Builder {
        private List<String> nodeNames;
        private List<String> lockTypes;

        public Builder setNodeNames(List<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        public Builder setLockTypes(List<String> lockTypes) {
            this.lockTypes = lockTypes;
            return this;
        }

        public NodeGroup build() {
            NodeGroup nodeGroup = new NodeGroup();
            nodeGroup.nodeNames = nodeNames;
            nodeGroup.lockTypes = lockTypes;
            return nodeGroup;
        }
    }
}
