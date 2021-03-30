package io.kyligence.kap.engine.spark.streaming.util;

import com.codahale.metrics.jmx.JmxReporter;
import io.kyligence.kap.common.metrics.MetricsController;
import io.kyligence.kap.common.metrics.MetricsInfluxdbReporter;
import io.kyligence.kap.common.metrics.MetricsReporter;
import lombok.val;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsManager {

  private static final Logger logger = LoggerFactory
      .getLogger(MetricsManager.class);

  public static void startReporter() throws Exception {
    val config = KylinConfig.getInstanceFromEnv();
    if (config.getStreamingMetricsEnabled()) {

      final MetricsReporter influxDbReporter = MetricsInfluxdbReporter.getInstance();
      influxDbReporter.init(KapConfig.getInstanceFromEnv());

      final JmxReporter jmxReporter = JmxReporter.forRegistry(MetricsController.getDefaultMetricRegistry()).build();
      jmxReporter.start();
    } else {
      logger.info("does not start monitoring based on configuration");
    }
  }
}
