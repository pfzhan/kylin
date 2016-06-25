package io.kyligence.kap.metadata.model;

import org.apache.kylin.metadata.model.IStorageAware;

/**
 * Created by roger on 5/27/16.
 */
public interface IKapStorageAware extends IStorageAware {
    int ID_SHARDED_PARQUET = 3;
}
