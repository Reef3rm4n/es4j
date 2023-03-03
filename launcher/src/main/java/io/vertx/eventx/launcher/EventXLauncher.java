package io.vertx.eventx.launcher;


import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;


public class EventXLauncher extends Launcher {

  public static void main(String[] args) {
    new EventXLauncher().dispatch(args);
  }


  @Override
  public void beforeStartingVertx(VertxOptions vertxOptions) {
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
      )
      .setEventBusOptions(
        new EventBusOptions()
//          .setLogActivity(true)
//          .setTcpQuickAck(true)
//          .setTcpFastOpen(true)
//          .setTcpNoDelay(true)
//          .setTcpCork(true)
//          .setUseAlpn(true)
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
