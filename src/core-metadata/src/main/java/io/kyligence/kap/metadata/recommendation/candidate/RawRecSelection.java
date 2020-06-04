package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;

import org.apache.kylin.common.KylinConfig;

public class RawRecSelection {
    private static RawRecSelection ourInstance = new RawRecSelection(KylinConfig.getInstanceFromEnv());

    public static RawRecSelection getInstance() {
        return ourInstance;
    }

    private KylinConfig config;

    private RawRecSelection(KylinConfig config) {
    }

    public List<RawRecItem> selectBestLayout(int topn, String model, String project) {
        return getStrategy().getBestRecItemByModel(topn, project, model);
    }

    public RecSelectStrategy getStrategy() {
        return new CostBasedRecSelectStrategy(config);
    }
}
