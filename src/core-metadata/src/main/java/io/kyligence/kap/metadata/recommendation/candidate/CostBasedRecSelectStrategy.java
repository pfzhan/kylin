package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;

import org.apache.kylin.common.KylinConfig;

public class CostBasedRecSelectStrategy implements RecSelectStrategy {
    private KylinConfig config;

    public CostBasedRecSelectStrategy(KylinConfig config) {
        this.config = config;
    }

    @Override
    public List<RawRecItem> getBestRecItem(int topn) {
        throw new RuntimeException("Not supported");
    }

    @Override
    public List<RawRecItem> getBestRecItemByModel(int topn, String project, String model) {
        RawRecommendationManager rawRecommendationManager = RawRecommendationManager.getInstance(config, project);
        return rawRecommendationManager.getCandidatesByModelAndBenifit(project, model, topn);
    }

    @Override
    public List<RawRecItem> getBestRecItemByProject(int topn, String project) {
        RawRecommendationManager rawRecommendationManager = RawRecommendationManager.getInstance(config, project);
        return rawRecommendationManager.getCandidatesByProjectAndBenifit(project, topn);
    }

    public void update(RawRecItem recItem, long time) {
        double latencyOfTheDay = recItem.getLayoutMetric().getLatencyMap().getLatencyByDate(time);
        recItem.setCost((recItem.getCost() + latencyOfTheDay) / Math.E);
    }
}
