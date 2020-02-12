package io.kyligence.kap.engine.spark.streaming.util;

import com.codahale.metrics.JmxReporter;
import io.kyligence.kap.common.metrics.NMetricsController;
import io.kyligence.kap.common.metrics.NMetricsInfluxdbReporter;
import io.kyligence.kap.common.metrics.NMetricsReporter;
import lombok.val;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

  private static final Logger logger = LoggerFactory
      .getLogger(MetricsManager.class);

  public static void startReporter() {
    val config = KylinConfig.getInstanceFromEnv();
    if (config.getStreamingMetricsEnabled()) {

      final NMetricsReporter influxDbReporter = new NMetricsInfluxdbReporter();
      influxDbReporter.init(KapConfig.getInstanceFromEnv());

      final JmxReporter jmxReporter = JmxReporter.forRegistry(NMetricsController.getDefaultMetricRegistry()).build();
      jmxReporter.start();
    } else {
      logger.info("does not start monitoring based on configuration");
    }
  }
}
