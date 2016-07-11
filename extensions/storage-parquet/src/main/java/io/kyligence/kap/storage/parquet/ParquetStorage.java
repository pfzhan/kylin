package io.kyligence.kap.storage.parquet;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

import io.kyligence.kap.storage.parquet.steps.ParquetMROutput2;

public class ParquetStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {

        if (realization.getType() != RealizationType.CUBE) {
            throw new IllegalStateException("");
        }

        return new io.kyligence.kap.storage.parquet.cube.CubeStorageQuery((CubeInstance) realization);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput2.class) {
            return (I) new ParquetMROutput2();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
