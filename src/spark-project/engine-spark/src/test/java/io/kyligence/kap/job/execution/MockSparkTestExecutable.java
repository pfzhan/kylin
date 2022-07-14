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

package io.kyligence.kap.job.execution;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;

import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class MockSparkTestExecutable extends NSparkExecutable {

    private String metaUrl;

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {

        NDataModelManager modelManager = NDataModelManager.getInstance(config, "default");
        Set<String> dumpSet = modelManager.listAllModelIds().stream()
                .map(this::getResourcePath).collect(Collectors.toSet());
        return dumpSet;
    }

    private String getResourcePath(String modelId) {
        return "/default" + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + modelId + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public String getDistMetaUrl() {
        return getMetaUrl();
    }

    public MockSparkTestExecutable() {
        super();
    }

    public MockSparkTestExecutable(Object notSetId) {
        super(notSetId);
    }
}
