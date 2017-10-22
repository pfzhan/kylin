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

import org.apache.kylin.metadata.model.PartitionDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.KapModel;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KapModelResponse extends KapModel {
    public void setProject(String project) {
        this.project = project;
    }

    public KapModelResponse() {

    }

    @JsonProperty("project")
    private String project;

    public KapModelResponse(KapModel model) {
        setUuid(model.getUuid());
        setLastModified(model.getLastModified());
        setVersion(model.getVersion());
        setName(model.getName());
        setOwner(model.getOwner());
        setDraft(model.isDraft());
        setDescription(model.getDescription());
        setRootFactTableName(model.getRootFactTableName());
        setJoinTables(model.getJoinTables());
        setDimensions(model.getDimensions());
        setMetrics(model.getMetrics());
        setFilterCondition(model.getFilterCondition());
        if (model.getPartitionDesc() != null)
            setPartitionDesc(PartitionDesc.getCopyOf(model.getPartitionDesc()));
        setCapacity(model.getCapacity());
        setComputedColumnDescs(model.getComputedColumnDescs());
        setMutiLevelPartitionColStrs(model.getMutiLevelPartitionColStrs());
    }
}
