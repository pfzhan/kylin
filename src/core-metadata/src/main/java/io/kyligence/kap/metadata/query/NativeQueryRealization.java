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

package io.kyligence.kap.metadata.query;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.metadata.acl.NDataModelAclParams;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class NativeQueryRealization implements Serializable {
    private String modelId;
    private String modelAlias;
    private Long layoutId;
    private String indexType;
    private boolean isPartialMatchModel;
    private boolean isValid = true;
    private List<String> snapshots;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType,
            boolean isPartialMatchModel, List<String> snapshots) {
        this.modelId = modelId;
        this.modelAlias = modelAlias;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = isPartialMatchModel;
        this.snapshots = snapshots;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, List<String> snapshots) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.snapshots = snapshots;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.modelAlias = modelAlias;
        this.indexType = indexType;
        this.isPartialMatchModel = false;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, boolean isPartialMatchModel) {
        this.modelId = modelId;
        this.layoutId = layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = isPartialMatchModel;
    }
}