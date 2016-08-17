package io.kyligence.kap.cube.raw;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableSegment implements Comparable<RawTableSegment>, IStorageAware {

    @JsonBackReference
    private RawTableInstance rawTableInstance;
    @JsonProperty
    private CubeSegment cubeSegment;
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("date_range_start")
    private long dateRangeStart;
    @JsonProperty("date_range_end")
    private long dateRangeEnd;
    @JsonProperty("source_offset_start")
    private long sourceOffsetStart;
    @JsonProperty("source_offset_end")
    private long sourceOffsetEnd;
    @JsonProperty("status")
    private SegmentStatusEnum status;

    @JsonProperty("dictionaries")
    private ConcurrentHashMap<String, String> dictionaries;

    public RawTableInstance getRawTableInstance() {
        return rawTableInstance;
    }

    public void setRawTableInstance(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    public long getSourceOffsetStart() {
        return sourceOffsetStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    public long getSourceOffsetEnd() {
        return sourceOffsetEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public ConcurrentHashMap<String, String> getDictionaries() {
        return dictionaries;
    }

    public void setDictionaries(ConcurrentHashMap<String, String> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public CubeSegment getCubeSegment() {
        return cubeSegment;
    }

    public void setCubeSegment(CubeSegment cubeSegment) {
        this.cubeSegment = cubeSegment;
    }

    @Override
    public int getStorageType() {
        return rawTableInstance.getStorageType();
    }

    @Override
    public int compareTo(RawTableSegment other) {
        long comp = this.getSourceOffsetStart() - other.getSourceOffsetStart();
        if (comp != 0)
            return comp < 0 ? -1 : 1;

        comp = this.getSourceOffsetEnd() - other.getSourceOffsetEnd();
        if (comp != 0)
            return comp < 0 ? -1 : 1;
        else
            return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RawTableSegment other = (RawTableSegment) obj;
        if (rawTableInstance == null) {
            if (other.rawTableInstance != null)
                return false;
        } else if (!rawTableInstance.equals(other.rawTableInstance))
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (status != other.status)
            return false;
        return true;
    }
}
