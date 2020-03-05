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

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

@Data
public class ReloadTableAffectedModelContext {

    private String modelId = null;

    private boolean isBroken = false;

    private Set<String> columns = Sets.newHashSet();

    private Set<Integer> columnIds = Sets.newHashSet();

    private Set<String> computedColumns = Sets.newHashSet();

    private Set<Integer> dimensions = Sets.newHashSet();

    private Set<Integer> measures = Sets.newHashSet();

    private Set<Long> removedLayouts = Sets.newHashSet();

    private Set<Long> addLayouts = Sets.newHashSet();

    public ReloadTableAffectedModelContext(String modelId) {
        this.modelId = modelId;
    }

    public Set<Long> getIndexes() {
        return layoutsToIndexes(removedLayouts);
    }

    public static Set<Long> layoutsToIndexes(Set<Long> layouts) {
        if (CollectionUtils.isEmpty(layouts)) {
            return Sets.newHashSet();
        }
        return layouts.stream().map(id -> id / IndexEntity.INDEX_ID_STEP * IndexEntity.INDEX_ID_STEP)
                .collect(Collectors.toSet());
    }
}
