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

import static lombok.AccessLevel.PRIVATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
public class SchemaChangeCheckResult implements IKeep {

    @JsonProperty
    private Map<String, ModelSchemaChange> models = new HashMap<>();

    @Data
    public static class ModelSchemaChange implements IKeep {
        private int differences;

        @Setter(PRIVATE)
        @JsonProperty("missing_items")
        private List<ChangedItem> missingItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("new_items")
        private List<ChangedItem> newItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("update_items")
        private List<UpdatedItem> updateItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("reduce_items")
        private List<ChangedItem> reduceItems = new ArrayList<>();

        @JsonProperty("importable")
        public boolean importable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isImportable);
        }

        @JsonProperty("creatable")
        public boolean creatable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isCreatable);
        }

        @JsonProperty("overwritable")
        public boolean overwritable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isOverwritable);
        }

        public int getDifferences() {
            return missingItems.size() + newItems.size() + updateItems.size() + reduceItems.size();
        }

        @JsonProperty("has_same_name")
        public boolean hasSameName() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isHasSameName);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BaseItem implements IKeep {
        @JsonProperty("type")
        SchemaNodeType type;

        @JsonProperty("model_alias")
        String modelAlias;

        @JsonProperty("reason")
        UN_IMPORT_REASON reason;

        @JsonProperty("has_same_name")
        boolean hasSameName;

        @JsonProperty("importable")
        boolean importable;
        @JsonProperty("creatable")
        boolean creatable;
        @JsonProperty("overwritable")
        boolean overwritable;

        @JsonIgnore
        public String getDetail(SchemaNode schemaNode) {
            switch (schemaNode.getType()) {
            case TABLE_COLUMN:
                return schemaNode.getKey();
            case MODEL_DIMENSION:
            case MODEL_MEASURE:
                return (String) schemaNode.getAttributes().get("name");
            default:
                return schemaNode.getDetail();
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangedItem extends BaseItem {
        @Getter(PRIVATE)
        private SchemaNode schemaNode;

        public ChangedItem(SchemaNodeType type, SchemaNode schemaNode, String modelAlias, UN_IMPORT_REASON reason,
                boolean hasSameName, boolean importable, boolean creatable, boolean overwritable) {
            super(type, modelAlias, reason, hasSameName, importable, creatable, overwritable);
            this.schemaNode = schemaNode;
        }

        public static ChangedItem createUnImportableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                UN_IMPORT_REASON reason, boolean hasSameName) {
            return new ChangedItem(type, schemaNode, null, reason, hasSameName, false, false, false);
        }

        public static ChangedItem createUnImportableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                String modelAlias, UN_IMPORT_REASON reason, boolean hasSameName) {
            return new ChangedItem(type, schemaNode, modelAlias, reason, hasSameName, false, false, false);
        }

        public static ChangedItem createOverwritableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                boolean hasSameName) {
            return new ChangedItem(type, schemaNode, null, null, hasSameName, true, true, true);
        }

        public static ChangedItem createOverwritableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                String modelAlias, boolean hasSameName) {
            return new ChangedItem(type, schemaNode, modelAlias, null, hasSameName, true, true, true);
        }

        public static ChangedItem createCreatableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                boolean hasSameName) {
            return new ChangedItem(type, schemaNode, null, null, hasSameName, true, true, false);
        }

        public String getModelAlias() {
            return modelAlias != null ? modelAlias : schemaNode.getSubject();
        }

        public String getDetail() {
            return getDetail(schemaNode);
        }

        public Map<String, Object> getAttributes() {
            return schemaNode.getAttributes();
        }
    }

    @Data
    public static class UpdatedItem extends BaseItem {
        @JsonIgnore
        private SchemaNode firstSchemaNode;

        @JsonIgnore
        private SchemaNode secondSchemaNode;

        @JsonProperty("first_attributes")
        public Map<String, Object> getFirstAttributes() {
            return firstSchemaNode.getAttributes();
        }

        @JsonProperty("second_attributes")
        public Map<String, Object> getSecondAttributes() {
            return secondSchemaNode.getAttributes();
        }

        @JsonProperty("first_detail")
        public String getFirstDetail() {
            return getDetail(firstSchemaNode);
        }

        @JsonProperty("second_detail")
        public String getSecondDetail() {
            return getDetail(secondSchemaNode);
        }

        public UpdatedItem(SchemaNode firstSchemaNode, SchemaNode secondSchemaNode, String modelAlias,
                UN_IMPORT_REASON reason, boolean hasSameName, boolean importable, boolean creatable,
                boolean overwritable) {
            super(secondSchemaNode.getType(), modelAlias, reason, hasSameName, importable, creatable, overwritable);
            this.firstSchemaNode = firstSchemaNode;
            this.secondSchemaNode = secondSchemaNode;
        }

        public static UpdatedItem getSchemaUpdate(SchemaNode first, SchemaNode second, String modelAlias,
                UN_IMPORT_REASON reason, boolean hasSameName, boolean importable, boolean creatable,
                boolean overwritable) {
            return new UpdatedItem(first, second, modelAlias, reason, hasSameName, importable, creatable, overwritable);
        }

        public static UpdatedItem getSchemaUpdate(SchemaNode first, SchemaNode second, String modelAlias,
                boolean hasSameName, boolean importable, boolean creatable, boolean overwritable) {
            return getSchemaUpdate(first, second, modelAlias, UN_IMPORT_REASON.NONE, hasSameName, importable, creatable,
                    overwritable);
        }
    }

    public void addMissingItems(List<ChangedItem> missingItems) {
        missingItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getMissingItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    public void addNewItems(List<ChangedItem> newItems) {
        newItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getNewItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    public void addUpdateItems(List<UpdatedItem> updateItems) {
        updateItems.forEach(item -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(item.getModelAlias(), new ModelSchemaChange());
            modelSchemaChange.getUpdateItems().add(item);
            models.put(item.getModelAlias(), modelSchemaChange);
        });
    }

    public void addReduceItems(List<ChangedItem> reduceItems) {
        reduceItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getReduceItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    @JsonIgnore
    public void areEqual(List<String> modelAlias) {
        modelAlias.forEach(model -> models.putIfAbsent(model, new ModelSchemaChange()));
    }

    public enum UN_IMPORT_REASON {
        SAME_CC_NAME_HAS_DIFFERENT_EXPR, //
        DIFFERENT_CC_NAME_HAS_SAME_EXPR, //
        USED_UNLOADED_TABLE, //
        TABLE_COLUMN_DATATYPE_CHANGED, //
        MISSING_TABLE_COLUMN, //
        MISSING_TABLE, //
        NONE;
    }
}
