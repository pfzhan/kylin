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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.project;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.EqualsAndHashCode;
import lombok.val;

/**
 * Project is a concept in Kylin similar to schema in DBMS
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ProjectInstance extends RootPersistentEntity implements ISourceAware {

    private static final Logger logger = LoggerFactory.getLogger(NProjectManager.class);

    public static final String DEFAULT_PROJECT_NAME = "default";

    private KylinConfigExt config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("status")
    private ProjectStatusEnum status;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("description")
    private String description;

    @JsonProperty("ext_filters")
    private Set<String> extFilters = new TreeSet<String>();

    @EqualsAndHashCode.Include
    @JsonProperty("maintain_model_type")
    private MaintainModelType maintainModelType = MaintainModelType.AUTO_MAINTAIN;

    @JsonProperty("override_kylin_properties")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private LinkedHashMap<String, String> overrideKylinProps;

    @JsonProperty("push_down_range_limited")
    @Getter
    @Setter
    private boolean pushDownRangeLimited = true;

    @JsonProperty("segment_config")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Getter
    @Setter
    private SegmentConfig segmentConfig = new SegmentConfig(true, Lists.newArrayList(AutoMergeTimeEnum.WEEK,
            AutoMergeTimeEnum.MONTH, AutoMergeTimeEnum.YEAR), new VolatileRange(), new RetentionRange());

    @Override
    public String getResourcePath() {
        return concatResourcePath(resourceName());
    }

    public static String concatResourcePath(String projectName) {
        return ResourceStore.PROJECT_ROOT + "/" + projectName + MetadataConstants.FILE_SURFIX;
    }

    public static ProjectInstance create(String name, String owner, String description,
            LinkedHashMap<String, String> overrideProps, MaintainModelType maintainModelType) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.setName(name);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(description);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(System.currentTimeMillis());
        projectInstance.setOverrideKylinProps(overrideProps);
        if (maintainModelType != null) {
            projectInstance.setMaintainModelType(maintainModelType);
        }
        return projectInstance;
    }

    public void initConfig(KylinConfig config) {
        this.config = KylinConfigExt.createInstance(config, this.overrideKylinProps);
    }

    // ============================================================================

    public ProjectInstance() {
    }

    @Override
    public String resourceName() {
        return this.name;
    }

    public MaintainModelType getMaintainModelType() {
        return maintainModelType;
    }

    public void setMaintainModelType(MaintainModelType maintainModelType) {
        this.maintainModelType = maintainModelType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setExtFilters(Set<String> extFilters) {
        this.extFilters = extFilters;
    }

    public ProjectStatusEnum getStatus() {
        return status;
    }

    public void setStatus(ProjectStatusEnum status) {
        this.status = status;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ImmutableList<RealizationEntry> getRealizationEntries() {
        return ImmutableList.copyOf(getRealizationsFromResource(name));
    }

    public ImmutableList<String> getModels() {
        return ImmutableList.copyOf(getModelsFromResource(name));
    }

    public ImmutableSet<String> getTables() {
        return ImmutableSet.copyOf(getTableFromResource(name));
    }

    public boolean containsRealization(final String realizationType, final String realization) {
        return Iterables.any(getRealizationsFromResource(this.name), input -> input.getType().equals(realizationType)
                && input.getRealization().equalsIgnoreCase(realization));
    }

    public List<RealizationEntry> getRealizationEntries(final String realizationType) {
        if (realizationType == null)
            return getRealizationsFromResource(name);

        return ImmutableList.copyOf(Iterables.filter(getRealizationsFromResource(this.name),
                input -> input.getType().equals(realizationType)));
    }

    public int getRealizationCount(final String realizationType) {
        val realizationEntries = getRealizationsFromResource(this.name);
        if (realizationType == null)
            return realizationEntries.size();

        return Iterables.size(Iterables.filter(realizationEntries, input -> input.getType().equals(realizationType)));
    }

    public boolean containsTable(String tableName) {
        return getTableFromResource(name).contains(tableName.toUpperCase());
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean containsModel(String modelId) {
        List<String> models = getModelsFromResource(name);
        return models != null && models.contains(modelId);
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    public void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {
        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }
        this.overrideKylinProps = overrideKylinProps;
        if (config != null) {
            this.config = KylinConfigExt.createInstance(config.base(), overrideKylinProps);
        }
    }

    public KylinConfigExt getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public void init(KylinConfig config) {
        if (name == null)
            name = ProjectInstance.DEFAULT_PROJECT_NAME;

        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }

        initConfig(config);

        if (StringUtils.isBlank(this.name))
            throw new IllegalStateException("Project name must not be blank");
    }

    @Override
    public String toString() {
        return "ProjectDesc [name=" + name + "]";
    }

    @Override
    public int getSourceType() {
        return getConfig().getDefaultSource();
    }

    private List<String> getModelsFromResource(String projectName) {
        String modeldescRootPath = getProjectRootPath(projectName) + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT;
        Set<String> modelResource = getStore().listResources(modeldescRootPath);
        List<String> models = getNameListFromResource(modelResource);
        return models;
    }

    private String getProjectRootPath(String prj) {
        return "/" + prj;
    }

    private List<RealizationEntry> getRealizationsFromResource(String projectName) {
        String dataflowRootPath = getProjectRootPath(projectName) + ResourceStore.DATAFLOW_RESOURCE_ROOT;
        Set<String> realizationResource = getStore().listResources(dataflowRootPath);

        if (realizationResource == null)
            return new ArrayList<>();

        List<String> realizations = getNameListFromResource(realizationResource);
        List<RealizationEntry> realizationEntries = new ArrayList<>();
        for (String realization : realizations) {
            RealizationEntry entry = RealizationEntry.create("NCUBE", realization);
            realizationEntries.add(entry);
        }

        return realizationEntries;
    }

    private Set<String> getTableFromResource(String projectName) {
        String tableRootPath = getProjectRootPath(projectName) + ResourceStore.TABLE_RESOURCE_ROOT;
        Set<String> tableResource = getStore().listResources(tableRootPath);
        if (tableResource == null)
            return new TreeSet<>();
        List<String> tables = getNameListFromResource(tableResource);
        Set<String> tableSet = new TreeSet<>(tables);
        return tableSet;
    }

    //drop the path ahead name and drop suffix e.g [/default/model_desc/]nmodel_basic[.json]
    private List<String> getNameListFromResource(Set<String> modelResource) {
        if (modelResource == null)
            return new ArrayList<>();
        List<String> nameList = new ArrayList<>();
        for (String resource : modelResource) {
            String[] path = resource.split("/");
            resource = path[path.length - 1];
            resource = StringUtil.dropSuffix(resource, MetadataConstants.FILE_SURFIX);
            nameList.add(resource);
        }
        return nameList;
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

}
