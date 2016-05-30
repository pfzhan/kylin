package io.kyligence.kap.storage.parquet;

import io.kyligence.kap.engine.mr.IMROutput3;
import io.kyligence.kap.storage.parquet.steps.ParquetMROutput3Transition;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

public class ParquetStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput3.class) {
            return (I) new ParquetMROutput3Transition();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
