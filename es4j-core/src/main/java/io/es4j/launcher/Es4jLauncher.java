package io.es4j.launcher;


import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Es4jLauncher extends Launcher {

  private final Logger logger = LoggerFactory.getLogger(Es4jLauncher.class);

  public static void main(String[] args) {
    new Es4jLauncher().dispatch(args);
  }


  @Override
  public void beforeStartingVertx(VertxOptions vertxOptions) {
    logger.info("--- Starting Event.x -----");
    vertxOptions
      .setPreferNativeTransport(true)
      .setMetricsOptions(
        new MicrometerMetricsOptions()
          .setPrometheusOptions(new VertxPrometheusOptions()
            .setEnabled(true)
            .setPublishQuantiles(true)
          )
          .setJvmMetricsEnabled(true)
          .setEnabled(true)
      );
  }





  @Override
  public void afterStartingVertx(Vertx vertx) {
    PrometheusMeterRegistry registry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
    registry.config().meterFilter(
      new MeterFilter() {
        @Override
        public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
          return DistributionStatisticConfig.builder()
            .percentilesHistogram(true)
            .build()
            .merge(config);
        }
      });
  }
}
