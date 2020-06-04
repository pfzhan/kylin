package io.kyligence.kap.metadata.recommendation.candidate;

import java.util.List;

public interface RecSelectStrategy {

    List<RawRecItem> getBestRecItem(int topn);

    List<RawRecItem> getBestRecItemByModel(int topn, String project, String model);

    List<RawRecItem> getBestRecItemByProject(int topn, String project);
}
