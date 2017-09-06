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

package io.kyligence.kap.vube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class VubeInstance extends HybridInstance implements IRealization, IBuildable {
    public static final String VUBE_RESOURCE_ROOT = "/vube";

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("descriptor")
    private String descName;

    @JsonProperty("cubes")
    private List<CubeInstance> versionedCubes;

    @JsonProperty("rawtables")
    private List<RawTableInstance> versionedRawTables;

    @JsonProperty("sample_sqls")
    private List<List<String>> sampleSqls;

    @JsonProperty("cost")
    private int cost = 50;

    @JsonProperty("status")
    private RealizationStatusEnum status;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    private volatile CubeInstance[] cubes = null;
    private List<TblColRef> allDimensions = null;
    private Set<TblColRef> allColumns = null;
    private Set<ColumnDesc> allColumnDescs = null;
    private List<MeasureDesc> allMeasures = null;
    private long dateRangeStart;
    private long dateRangeEnd;
    private boolean isReady = false;

    private final static Logger logger = LoggerFactory.getLogger(io.kyligence.kap.vube.VubeInstance.class);

    public void setVersionedCubes(List<CubeInstance> versionedCubes) {
        this.versionedCubes = versionedCubes;
    }

    public List<CubeInstance> getVersionedCubes() {
        return versionedCubes;
    }

    public static VubeInstance create(String name, CubeInstance cube) {
        VubeInstance vubeInstance = new VubeInstance();
        List<CubeInstance> cubeList = new ArrayList<>();

        cubeList.add(cube);
        vubeInstance.setConfig(cube.getConfig());
        vubeInstance.setName(name);
        vubeInstance.setDescName(cube.getName());
        vubeInstance.setCreateTimeUTC(System.currentTimeMillis());

        if (cube.getSegments().size() > 0) {
            vubeInstance.setStatus(cube.getStatus());
        } else {
            vubeInstance.setStatus(RealizationStatusEnum.DESCBROKEN);
        }
        vubeInstance.setVersionedCubes(cubeList);
        vubeInstance.versionedRawTables = new ArrayList<>();
        vubeInstance.sampleSqls = new ArrayList<>();
        vubeInstance.updateRandomUuid();

        return vubeInstance;
    }

    private void init() {
        synchronized (this) {
            if (versionedCubes == null || versionedCubes.size() == 0)
                return;

            List<CubeInstance> cubeList = Lists.newArrayList();
            for (int i = 0; i < versionedCubes.size(); i++) {
                CubeInstance cubeInstance = versionedCubes.get(i);
                if (cubeInstance == null) {
                    logger.error("Realization '" + cubeInstance.getName() + " is not found, remove from Vube '"
                            + this.getName() + "'");
                    continue;
                }
                if (cubeInstance.isReady() == false) {
                    logger.error("Cube '" + cubeInstance.getName() + " is disabled, remove from Vube '" + this.getName()
                            + "'");
                    continue;
                }
                cubeList.add(cubeInstance);
            }

            LinkedHashSet<TblColRef> columns = new LinkedHashSet<TblColRef>();
            LinkedHashSet<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
            LinkedHashSet<MeasureDesc> measures = new LinkedHashSet<MeasureDesc>();
            dateRangeStart = 0;
            dateRangeEnd = Long.MAX_VALUE;
            for (CubeInstance cube : cubeList) {
                columns.addAll(cube.getAllColumns());
                dimensions.addAll(cube.getAllDimensions());
                measures.addAll(cube.getMeasures());

                if (cube.isReady())
                    isReady = true;

                if (dateRangeStart == 0 || cube.getDateRangeStart() < dateRangeStart)
                    dateRangeStart = cube.getDateRangeStart();

                if (dateRangeStart == Long.MAX_VALUE || cube.getDateRangeEnd() > dateRangeEnd)
                    dateRangeEnd = cube.getDateRangeEnd();
            }

            allDimensions = Lists.newArrayList(dimensions);
            allColumns = columns;
            allColumnDescs = asColumnDescs(allColumns);
            allMeasures = Lists.newArrayList(measures);

            Collections.sort(cubeList, new Comparator<CubeInstance>() {
                @Override
                public int compare(CubeInstance o1, CubeInstance o2) {

                    long i1 = o1.getDateRangeStart();
                    long i2 = o2.getDateRangeStart();
                    long comp = i1 - i2;
                    if (comp != 0) {
                        return comp > 0 ? 1 : -1;
                    }

                    i1 = o1.getDateRangeEnd();
                    i2 = o2.getDateRangeEnd();
                    comp = i1 - i2;
                    if (comp != 0) {
                        return comp > 0 ? 1 : -1;
                    }

                    return 0;
                }
            });

            this.cubes = cubeList.toArray(new CubeInstance[cubeList.size()]);
        }
    }

    private Set<ColumnDesc> asColumnDescs(Set<TblColRef> columns) {
        LinkedHashSet<ColumnDesc> result = new LinkedHashSet<>();
        for (TblColRef col : columns) {
            result.add(col.getColumnDesc());
        }
        return result;
    }

    public Segments<CubeSegment> getAllSegments() {
        Segments<CubeSegment> segments = new Segments();

        for (CubeInstance cube: getVersionedCubes()) {
            segments.addAll(cube.getSegments());
        }
        return segments;
    }

    public Map<String, Segments<CubeSegment>> getVersionSegments() {
        Map<String, Segments<CubeSegment>> versionSegments = new HashMap<>();

        for (CubeInstance cube : getVersionedCubes()) {
            if (cube.getName().equals(this.getName())) {
                versionSegments.put("version_0", cube.getSegments());
            } else if((cube.getName().startsWith(this.getName()) && cube.getName().contains("_version_"))){
                String version = cube.getName().substring(cube.getName().indexOf("_version_") + 1);
                versionSegments.put(version, cube.getSegments());
            }
            versionSegments.put(cube.getName(), cube.getSegments());
        }

        return versionSegments;
    }

    public Segments<CubeSegment> getSegments(List<String> segNames) {
        Segments<CubeSegment> neededSegments = new Segments();

        for (CubeInstance cube : this.getVersionedCubes()) {
            Segments<CubeSegment> segments = cube.getSegments();

            for (CubeSegment segment : segments) {
                if (segNames.contains(segment.getName())) {
                    neededSegments.add(segment);
                }
            }
        }

        return neededSegments;
    }

    public Segments<CubeSegment> getSegments(String version) {
        Segments<CubeSegment> segments = new Segments();
        List<CubeInstance> cubes = this.getVersionedCubes();

        // Updated metadata
        if (version.equals("version_0")) {
            if (cubes.size() > 0 && (!cubes.get(0).getName().contains("_version_"))) {
                return cubes.get(0).getSegments();
            }
        }

        for (CubeInstance cube : getVersionedCubes()) {
            if (cube.getName().endsWith(version)) {
                segments.addAll(cube.getSegments());
                break;
            }
        }

        return segments;
    }

    public List<RawTableInstance> getVersionedRawTables() {
        return versionedRawTables;
    }

    public void setVersionedRawTables(List<RawTableInstance> versionedRawTables) {
        this.versionedRawTables = versionedRawTables;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.cost = Integer.MAX_VALUE;

        for (CubeInstance cubeInstance : getVersionedCubes()) {
            CapabilityResult child = cubeInstance.isCapable(digest);
            if (child.capable) {
                result.capable = true;
                result.cost = Math.min(result.cost, child.cost);
                result.influences.addAll(child.influences);
            }
        }

        return result;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.HYBRID2;
    }

    @Override
    public DataModelDesc getModel() {
        if (this.getLatestCube() != null)
            return this.getLatestCube().getModel();
        return null;
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        init();
        return allColumns;
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        init();
        return allColumnDescs;
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        init();
        return allMeasures;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String toString() {
        return getCanonicalName();
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    @Override
    public long getDateRangeStart() {
        return dateRangeStart;
    }

    @Override
    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    @Override
    public boolean supportsLimitPushDown() {
        return false;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        init();
        return allDimensions;
    }

    @Override
    public IRealization[] getRealizations() {
        init();
        return cubes;
    }

    public CubeInstance[] getCubes() {
        init();
        return cubes;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String vubeName) {
        return VUBE_RESOURCE_ROOT + "/" + vubeName + ".json";
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public int getCost() {
        return cost;
    }

    public CubeInstance getLatestCube() {
        if (getVersionedCubes().size() > 0) {
            return versionedCubes.get(versionedCubes.size() - 1);
        }
        return null;
    }

    public CubeInstance getLatestReadyCube() {
        CubeInstance cube = null;

        for (int i = versionedCubes.size() - 1; i >=0; i--) {
            if(versionedCubes.get(i).getStatus() == RealizationStatusEnum.READY) {
                cube = versionedCubes.get(i);
            }
        }

        return cube;
    }

    public CubeInstance getCubeWithVersion(String version) {
        CubeInstance result = null;

        if (version.equals("version_0")) {
            if (versionedCubes.size() > 0 && !versionedCubes.get(0).getName().contains("_version_")) {
                result = versionedCubes.get(0);
            }
        } else {
            for (CubeInstance cube : versionedCubes) {
                if (cube.getName().equals(version)) {
                    result = cube;
                    break;
                }
            }
        }

        return result;
    }

    public List<String> getSampleSqlsWithVersion(String version) {
        List<String> result = null;
        int idx = -1;

        if (version == null || version.length() == 0) {
            idx = sampleSqls.size() - 1;
        } else if (version.equals("version_0")) {
            if (versionedCubes.size() > 0 && !versionedCubes.get(0).getName().contains("_version_")) {
                idx = 0;
            }
        } else {
            for (CubeInstance cube : versionedCubes) {
                if (cube.getName().equals(version)) {
                    break;
                }
                idx++;
            }
        }

        if (idx >= 0 && sampleSqls.size() > idx) {
            result = sampleSqls.get(idx);
        }

        return result;
    }

    @Override
    public int getStorageType() {
        return ID_HYBRID;
    }

    public String getDescName() {
        return descName;
    }

    public void setDescName(String descName) {
        this.descName = descName;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    @Override
    public int getEngineType() {
        CubeInstance latestCube = this.getLatestCube();

        if (latestCube != null) {
            return latestCube.getDescriptor().getEngineType();
        } else {
            return -1;
        }
    }

    @Override
    public int getSourceType() {
        return getModel().getRootFactTable().getTableDesc().getSourceType();
    }

    public List<List<String>> getSampleSqls() {
        return sampleSqls;
    }

    public void setSampleSqls(List<List<String>> sampleSqls) {
        this.sampleSqls = sampleSqls;
    }

    public static String cubeNameToVersion(VubeInstance vube, String cubeName) {
        String version = null;

        if (cubeName.equals(vube.getName())) {
            version = "version_0";
        } else if (cubeName.startsWith(vube.getName()) && cubeName.contains("_version_")) {
            version = cubeName.substring(cubeName.indexOf("_version_") + 1);
        }

        return version;
    }
}
