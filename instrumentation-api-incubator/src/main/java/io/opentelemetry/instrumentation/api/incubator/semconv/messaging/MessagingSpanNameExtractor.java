/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.incubator.semconv.messaging;

import static java.util.Objects.requireNonNull;

import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;

public final class MessagingSpanNameExtractor<REQUEST> implements SpanNameExtractor<REQUEST> {

  /**
   * Returns a {@link SpanNameExtractor} that constructs the span name according to <a
   * href="https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/docs/messaging/messaging-spans.md#span-name">
   * messaging semantic conventions</a>.
   *
   * @param operationName the system-specific name of the operation, used as the {@code <operation
   *     name>} part of the span name, e.g. {@code send}, {@code poll} or {@code ack}.
   * @see MessagingAttributesGetter#getDestination(Object) used to extract {@code <destination
   *     name>}.
   */
  public static <REQUEST> SpanNameExtractor<REQUEST> create(
      MessagingAttributesGetter<REQUEST, ?> getter,
      MessagingOperationType operationType,
      String operationName) {
    requireNonNull(operationType, "operationType");
    return new MessagingSpanNameExtractor<>(getter, requireNonNull(operationName, "operationName"));
  }

  private final MessagingAttributesGetter<REQUEST, ?> getter;
  private final String operationName;

  MessagingSpanNameExtractor(MessagingAttributesGetter<REQUEST, ?> getter, String operationName) {
    this.getter = getter;
    this.operationName = operationName;
  }

  @Override
  public String extract(REQUEST request) {
    // 对于rocketmq来说这里固定返回的是null
    String destinationName = getter.getDestinationTemplate(request);
    if (destinationName == null
        // rocketmq中isTemporaryDestination默认返回false
        && !getter.isTemporaryDestination(request)
        // rocketmq中isAnonymousDestination默认返回false
        && !getter.isAnonymousDestination(request)) {
      // 返回的是具体的topic
      destinationName = getter.getDestination(request);
    }
    // 如果destinationName为空，直接返回send，否则返回的是send 实际topic
    return destinationName == null ? operationName : operationName + " " + destinationName;
  }
}
