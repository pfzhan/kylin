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

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ISourceAware;

/**
 * Project is a concept in Kylin similar to schema in DBMS
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ProjectInstance extends RootPersistentEntity implements ISourceAware {

    public static final String DEFAULT_PROJECT_NAME = "default";
    private KylinConfigExt config;

    @JsonProperty("name")
    private String name;

    private Set<String> tables = new TreeSet<String>();

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("status")
    private ProjectStatusEnum status;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("last_update_time")
    // FIXME why not RootPersistentEntity.lastModified??
    private String lastUpdateTime;

    @JsonProperty("description")
    private String description;

    private List<RealizationEntry> realizationEntries;

    private List<String> models;

    @JsonProperty("ext_filters")
    private Set<String> extFilters = new TreeSet<String>();

    @JsonProperty("override_kylin_properties")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private LinkedHashMap<String, String> overrideKylinProps;


    public String getResourcePath() {
        return concatResourcePath(resourceName());
    }

    public static String concatResourcePath(String projectName) {
        return "/" + projectName + "/" + MetadataConstants.PROJECT_RESOURCE + MetadataConstants.FILE_SURFIX;
    }

    public static ProjectInstance create(String name, String owner, String description,
            LinkedHashMap<String, String> overrideProps, List<RealizationEntry> realizationEntries,
            List<String> models) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.updateRandomUuid();
        projectInstance.setName(name);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(description);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(System.currentTimeMillis());
        projectInstance.setOverrideKylinProps(overrideProps);

        if (realizationEntries != null)
            projectInstance.setRealizationEntries(realizationEntries);
        else
            projectInstance.setRealizationEntries(Lists.<RealizationEntry> newArrayList());
        if (models != null)
            projectInstance.setModels(models);
        else
            projectInstance.setModels(new ArrayList<String>());
        return projectInstance;
    }

    public void initConfig() {
        this.config = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), this.overrideKylinProps);
    }

    // ============================================================================

    public ProjectInstance() {
    }

    @Override
    public String resourceName() {
        return this.name;
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

    public boolean containsRealization(final String realizationType, final String realization) {
        return Iterables.any(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType().equals(realizationType) && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    public void removeRealization(final String type, final String realization) {
        Iterables.removeIf(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType().equals(type) && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    public List<RealizationEntry> getRealizationEntries(final String realizationType) {
        if (realizationType == null)
            return getRealizationEntries();

        return ImmutableList.copyOf(Iterables.filter(realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(@Nullable RealizationEntry input) {
                return input.getType().equals(realizationType);
            }
        }));
    }

    public int getRealizationCount(final String realizationType) {
        if (realizationType == null)
            return this.realizationEntries.size();

        return Iterables.size(Iterables.filter(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType().equals(realizationType);
            }
        }));
    }

    public void addRealizationEntry(final String realizationType, final String realizationName) {
        RealizationEntry pdm = new RealizationEntry();
        pdm.setType(realizationType);
        pdm.setRealization(realizationName);
        this.realizationEntries.add(pdm);
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public boolean containsTable(String tableName) {
        return tables.contains(tableName.toUpperCase());
    }

    public void removeTable(String tableName) {
        tables.remove(tableName.toUpperCase());
    }

    public void addExtFilter(String extFilterName) {
        this.getExtFilters().add(extFilterName);
    }

    public void removeExtFilter(String filterName) {
        extFilters.remove(filterName);
    }

    public void addTable(String tableName) {
        tables.add(tableName.toUpperCase());
    }

    public Set<String> getTables() {
        return tables;
    }

    public Set<String> getExtFilters() {
        return extFilters;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void recordUpdateTime(long timeMillis) {
        this.lastUpdateTime = formatTime(timeMillis);
    }

    public List<RealizationEntry> getRealizationEntries() {
        return realizationEntries;
    }

    public void setRealizationEntries(List<RealizationEntry> entries) {
        this.realizationEntries = entries;
    }

    public List<String> getModels() {
        return models;
    }

    public boolean containsModel(String modelName) {
        return models != null && models.contains(modelName);
    }

    public void setModels(List<String> models) {
        this.models = models;
    }

    public void addModel(String modelName) {
        if (this.getModels() == null) {
            this.setModels(new ArrayList<String>());
        }
        this.getModels().add(modelName);
    }

    public void removeModel(String modelName) {
        if (this.getModels() != null) {
            this.getModels().remove(modelName);
        }
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    public void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {
        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }
        this.overrideKylinProps = overrideKylinProps;
        initConfig();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public void init() {
        if (name == null)
            name = ProjectInstance.DEFAULT_PROJECT_NAME;

        if (realizationEntries == null) {
            realizationEntries = new ArrayList<RealizationEntry>();
        }

        if (tables == null)
            tables = new TreeSet<String>();

        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }

        initConfig();

        if (StringUtils.isBlank(this.name))
            throw new IllegalStateException("Project name must not be blank");
    }

    @Override
    public String toString() {
        return "ProjectDesc [name=" + name + "]";
    }

    public String getProjectResourcePath() {
        return "/" + name + "/" + MetadataConstants.PROJECT_RESOURCE + MetadataConstants.FILE_SURFIX;
    }

    @Override
    public int getSourceType() {
        return getConfig().getDefaultSource();
    }
}
