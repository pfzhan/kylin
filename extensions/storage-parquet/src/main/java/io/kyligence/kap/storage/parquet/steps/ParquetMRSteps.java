package io.kyligence.kap.storage.parquet.steps;

import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.metadata.realization.IRealizationSegment;

public class ParquetMRSteps extends JobBuilderSupport {
    public ParquetMRSteps(IRealizationSegment seg) {
        super(seg, null);
    }
}
