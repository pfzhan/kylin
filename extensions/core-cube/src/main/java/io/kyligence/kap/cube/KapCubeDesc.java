package io.kyligence.kap.cube;

import io.kyligence.kap.metadata.model.IKapStorageAware;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by roger on 5/27/16.
 */
public class KapCubeDesc extends CubeDesc{
    private List<DictionaryDesc> dictionary;
    private LinkedHashMap<String, String> overrideKylinProps;
    private KylinConfigExt config;

    public boolean isEnableSharding() {
        //in the future may extend to other storage that is shard-able
        return getStorageType() == IKapStorageAware.ID_SHARDED_PARQUET ||
                getStorageType() == IKapStorageAware.ID_SHARDED_HBASE;
    }

    public KapCubeDesc (CubeDesc cubeDesc) {
        super.setName(cubeDesc.getName());
        super.setModelName(cubeDesc.getModelName());
        super.setDescription(cubeDesc.getDescription());
        super.setNullStrings(cubeDesc.getNullStrings());
        super.setDimensions(cubeDesc.getDimensions());
        super.setMeasures(cubeDesc.getMeasures());
        //super.setDictionaries(cubeDesc.getDictionaries());
        super.setRowkey(cubeDesc.getRowkey());
        super.setHbaseMapping(cubeDesc.getHbaseMapping());
        super.setSignature(cubeDesc.getSignature());
        super.setNotifyList(cubeDesc.getNotifyList());
        super.setStatusNeedNotify(cubeDesc.getStatusNeedNotify());
        super.setAutoMergeTimeRanges(cubeDesc.getAutoMergeTimeRanges());
        super.setPartitionDateStart(cubeDesc.getPartitionDateStart());
        super.setPartitionDateEnd(cubeDesc.getPartitionDateEnd());
        super.setRetentionRange(cubeDesc.getRetentionRange());
        super.setEngineType(cubeDesc.getEngineType());
        super.setStorageType(cubeDesc.getStorageType());
        super.setAggregationGroups(cubeDesc.getAggregationGroups());
        //super.setOverrideKylinProps(cubeDesc.getOverrideKylinProps());
        //super.setConfig((KylinConfigExt) cubeDesc.getConfig());

        dictionary = cubeDesc.getDictionaries();
        overrideKylinProps = cubeDesc.getOverrideKylinProps();
        config = (KylinConfigExt) cubeDesc.getConfig();

        super.updateRandomUuid();
    }

    public List<DictionaryDesc> getDictionaries() {
        return dictionary;
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    public KylinConfig getConfig() {
        return config;
    }
}
