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
package io.kyligence.kap.metadata.model.schema;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

@Data
@ToString
public class ReloadTableContext {

    private Map<String, AffectedModelContext> removeAffectedModels = Maps.newHashMap();

    private Map<String, AffectedModelContext> changeTypeAffectedModels = Maps.newHashMap();

    private Set<String> favoriteQueries = Sets.newHashSet();

    private Set<String> addColumns = Sets.newHashSet();

    private Set<String> removeColumns = Sets.newHashSet();

    private Set<String> changeTypeColumns = Sets.newHashSet();

    private Set<String> duplicatedColumns = Sets.newHashSet();

    private Set<String> effectedJobs = Sets.newHashSet();

    private TableDesc tableDesc;

    private TableExtDesc tableExtDesc;

    public AffectedModelContext getRemoveAffectedModel(String project, String modelId) {
        return removeAffectedModels.getOrDefault(modelId,
                new AffectedModelContext(project, modelId, Sets.newHashSet(), true));
    }

    public AffectedModelContext getChangeTypeAffectedModel(String project, String modelId) {
        return changeTypeAffectedModels.getOrDefault(modelId,
                new AffectedModelContext(project, modelId, Sets.newHashSet(), false));
    }

    @Getter(lazy = true)
    private final Set<String> removeColumnFullnames = initRemoveColumnFullNames();

    private Set<String> initRemoveColumnFullNames() {
        return removeColumns.stream().map(col -> {
            assert tableDesc != null;
            return tableDesc.getName() + "." + col;
        }).collect(Collectors.toSet());
    }

    public boolean isChanged(TableDesc originTableDesc) {
        if (tableDesc.getColumns().length != originTableDesc.getColumns().length) {
            return true;
        } else {
            for (int i = 0; i < tableDesc.getColumns().length; i++) {
                val newCol = tableDesc.getColumns()[i];
                val originCol = originTableDesc.getColumns()[i];
                if (!Objects.equals(newCol.getName(), originCol.getName())
                        || !Objects.equals(newCol.getDatatype(), originCol.getDatatype())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isOnlyAddCols() {
        return removeColumns.isEmpty() && changeTypeColumns.isEmpty();
    }

    public boolean isNeedProcess() {
        return CollectionUtils.isNotEmpty(addColumns) || CollectionUtils.isNotEmpty(removeColumns)
                || CollectionUtils.isNotEmpty(changeTypeColumns);
    }
}
