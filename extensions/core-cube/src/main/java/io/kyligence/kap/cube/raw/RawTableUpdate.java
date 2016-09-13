package io.kyligence.kap.cube.raw;

import org.apache.kylin.metadata.realization.RealizationStatusEnum;

public class RawTableUpdate {
    private RawTableInstance rawTableInstance;
    private RawTableSegment[] toAddSegs = null;
    private RawTableSegment[] toRemoveSegs = null;
    private RawTableSegment[] toUpdateSegs = null;
    private RealizationStatusEnum status;
    private String owner;
    private int cost = -1;

    public RawTableUpdate(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
    }

    public RawTableInstance getRawTableInstance() {
        return rawTableInstance;
    }

    public RawTableUpdate setRawTableInstance(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
        return this;
    }

    public RawTableSegment[] getToAddSegs() {
        return toAddSegs;
    }

    public RawTableUpdate setToAddSegs(RawTableSegment... toAddSegs) {
        this.toAddSegs = toAddSegs;
        return this;
    }

    public RawTableSegment[] getToRemoveSegs() {
        return toRemoveSegs;
    }

    public RawTableUpdate setToRemoveSegs(RawTableSegment... toRemoveSegs) {
        this.toRemoveSegs = toRemoveSegs;
        return this;
    }

    public RawTableSegment[] getToUpdateSegs() {
        return toUpdateSegs;
    }

    public RawTableUpdate setToUpdateSegs(RawTableSegment... toUpdateSegs) {
        this.toUpdateSegs = toUpdateSegs;
        return this;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public RawTableUpdate setStatus(RealizationStatusEnum status) {
        this.status = status;
        return this;
    }

    public String getOwner() {
        return owner;
    }

    public RawTableUpdate setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public int getCost() {
        return cost;
    }

    public RawTableUpdate setCost(int cost) {
        this.cost = cost;
        return this;
    }
}
