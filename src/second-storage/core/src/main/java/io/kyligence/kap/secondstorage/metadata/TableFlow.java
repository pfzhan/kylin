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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.metadata.annotation.DataDefinition;

@DataDefinition
public class TableFlow extends RootPersistentEntity
        implements Serializable,
        HasLayoutElement<TableData>,
        IManagerAware<TableFlow> {

    public static final class Builder {
        private String model;

        public Builder setModel(String model) {
            this.model = model;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        private String description;

        public TableFlow build() {
            TableFlow result = new TableFlow();
            result.setUuid(model);
            result.setDescription(description);
            return result;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    protected transient Manager<TableFlow> manager;
    @Override
    public void setManager(Manager<TableFlow> manager) {
        this.manager = manager;
    }

    @Override
    public void verify() {
        // Here we check everything is ok
    }

    @JsonProperty("description")
    private String description;

    @JsonManagedReference
    @JsonProperty("data_list")
    private final List<TableData> tableDataList = Lists.newArrayList();

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public List<TableData> getTableDataList() {
        return Collections.unmodifiableList(tableDataList);
    }

    @Override
    public List<TableData> all() {
        return tableDataList;
    }

    public void upsertTableData(LayoutEntity layoutEntity, Consumer<TableData> updater, Supplier<TableData> creator) {
        checkIsNotCachedAndShared();
        TableData data = getEntity(layoutEntity)
                .map(tableData -> {
                    Preconditions.checkArgument(containIndex(layoutEntity, true));
                    updater.accept(tableData);
                    return tableData;})
                .orElseGet(() -> {
                    TableData newData = creator.get();
                    tableDataList.add(newData);
                    updater.accept(newData);
                    return newData;});
        Preconditions.checkArgument(HasLayoutElement.sameLayout(data, layoutEntity));
    }

    public void cleanTableData(Predicate<? super TableData> filter) {
        if (filter == null) {
            return;
        }

        checkIsNotCachedAndShared();
        this.tableDataList.removeIf(filter);
    }

    public void cleanTableData() {
        checkIsNotCachedAndShared();
        this.tableDataList.clear();
    }

    public void removeNodes(List<String> nodeNames) {
        if (CollectionUtils.isEmpty(nodeNames)) {
            return;
        }

        checkIsNotCachedAndShared();
        this.tableDataList.forEach(tableData -> tableData.removeNodes(nodeNames));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableFlow)) return false;
        if (!super.equals(o)) return false;

        TableFlow tableFlow = (TableFlow) o;

        if (!Objects.equals(description, tableFlow.description))
            return false;
        return Objects.equals(tableDataList, tableFlow.tableDataList);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + tableDataList.hashCode();
        return result;
    }

    public TableFlow update(Consumer<TableFlow> updater) {
        Preconditions.checkArgument(manager != null);
        return manager.update(uuid, updater);
    }
}
