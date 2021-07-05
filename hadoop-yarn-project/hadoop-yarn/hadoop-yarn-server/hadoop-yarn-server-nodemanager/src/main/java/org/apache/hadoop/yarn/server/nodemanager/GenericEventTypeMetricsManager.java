package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.metrics.GenericEventTypeMetrics;

import static org.apache.hadoop.metrics2.lib.Interns.info;

public final class GenericEventTypeMetricsManager {

  private GenericEventTypeMetricsManager() {
      // nothing to do
  }

  // Construct a GenericEventTypeMetrics for dispatcher
  public static <T extends Enum<T>> GenericEventTypeMetrics
      create(String dispatcherName, Class<T> eventTypeClass) {
    return new GenericEventTypeMetrics.EventTypeMetricsBuilder<T>()
        .setMs(DefaultMetricsSystem.instance())
        .setInfo(info("GenericEventTypeMetrics for " + eventTypeClass.getName(),
  "Metrics for " + dispatcherName))
        .setEnumClass(eventTypeClass)
        .setEnums(eventTypeClass.getEnumConstants())
        .build().registerMetrics();
    }
}