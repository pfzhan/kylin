package io.kyligence.kap.cube.raw;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class RawTableSegment implements Comparable<RawTableSegment>, IStorageAware {

    private RawTableInstance rawTableInstance;
    private CubeSegment cubeSegment;
    private Long shardNumber;

    public RawTableSegment(RawTableInstance rawTable, CubeSegment seg) {
        this.rawTableInstance = rawTable;
        this.cubeSegment = seg;
    }

    public static RawTableSegment getInstance(CubeSegment seg) {
        return new RawTableSegment(new RawTableInstance(seg.getCubeInstance()), seg);
    }

    public void setShardNum(Long num) {
        this.shardNumber = num;
    }

    public Long getShardNum() {
        return this.shardNumber;
    }

    public RawTableInstance getRawTableInstance() {
        return rawTableInstance;
    }

    public String getName() {
        return cubeSegment.getName();
    }

    public long getDateRangeStart() {
        return cubeSegment.getDateRangeStart();
    }

    public long getDateRangeEnd() {
        return cubeSegment.getDateRangeEnd();
    }

    public long getSourceOffsetStart() {
        return cubeSegment.getSourceOffsetStart();
    }

    public long getSourceOffsetEnd() {
        return cubeSegment.getSourceOffsetEnd();
    }

    public SegmentStatusEnum getStatus() {
        return cubeSegment.getStatus();
    }

    public CubeSegment getCubeSegment() {
        return cubeSegment;
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cubeSegment == null) ? 0 : cubeSegment.hashCode());
        return result;
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
        if (cubeSegment == null) {
            if (other.cubeSegment != null)
                return false;
        } else if (!cubeSegment.equals(other.cubeSegment))
            return false;
        return true;
    }

}
