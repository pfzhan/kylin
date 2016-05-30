package io.kyligence.kap.storage;

import io.kyligence.kap.common.KapKylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

import java.util.Map;

/**
 * Created by roger on 5/30/16.
 */
public class KapStorageFactory{
    private static ImplementationSwitch<IStorage> storages;
    static {
        Map<Integer, String> impls = KapKylinConfig.getInstanceFromEnv().getStorageEngines();
        storages = new ImplementationSwitch<IStorage>(impls, IStorage.class);
    }

    public static IStorage storage(IStorageAware aware) {
        return storages.get(aware.getStorageType());
    }

    public static IStorageQuery createQuery(IRealization realization) {
        return storage(realization).createQuery(realization);
    }

    public static <T> T createEngineAdapter(IStorageAware aware, Class<T> engineInterface) {
        return storage(aware).adaptToBuildEngine(engineInterface);
    }

}
