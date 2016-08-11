package io.kyligence.kap.metadata.model;

import org.apache.kylin.metadata.model.IStorageAware;

public interface IKapStorageAware extends IStorageAware {
    int ID_SHARDED_PARQUET = 3;
    int ID_RAWTABLE_SHARDED_PARQUET = 4;
}
