/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingProcessExceptionEventExtractor;
import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingReceiveExceptionEventExtractor;
import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingSendExceptionEventExtractor;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingConsumerMetrics;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingOperationType;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingProcessMetrics;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingProducerMetrics;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanKindExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingProcessInstrumenterFactory;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingSchemaUrls;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusExtractor;
import java.util.List;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/**
 * 组装 Send、Receive、Process 三种操作的名称、属性、指标、错误和传播策略。
 *
 * <p>Getter 提供 SDK 字段，通用 Messaging 提取器负责映射消息语义；RocketMQ 专用提取器补充 namespace、tag、message group
 * 等字段。Advice 和回调负责调用 start/end，工厂不管理单次操作状态。
 */
final class RocketMqInstrumenterFactory {
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.rocketmq-client-5.0";

  // messaging.operation.name values, named after the RocketMQ API operations
  private static final String SEND_OPERATION_NAME = "send";
  private static final String RECEIVE_OPERATION_NAME = "receive";
  private static final String PROCESS_OPERATION_NAME = "process";

  private RocketMqInstrumenterFactory() {}

  public static Instrumenter<PublishingMessageImpl, SendReceiptImpl> createProducerInstrumenter(
      OpenTelemetry openTelemetry, IncludeExclude headers) {
    RocketMqProducerAttributeGetter getter = new RocketMqProducerAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.SEND;

    AttributesExtractor<PublishingMessageImpl, SendReceiptImpl> attributesExtractor =
        buildMessagingAttributesExtractor(getter, operationType, SEND_OPERATION_NAME, headers);

    InstrumenterBuilder<PublishingMessageImpl, SendReceiptImpl> instrumenterBuilder =
        Instrumenter.<PublishingMessageImpl, SendReceiptImpl>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(getter, operationType, SEND_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(new RocketMqProducerAttributeExtractor())
            .addOperationMetrics(MessagingProducerMetrics.get());
    setMessagingSendExceptionEventExtractor(instrumenterBuilder);
    // Producer Instrumenter 的 start 会把新建 Span 的上下文注入消息。
    // Setter 先写入附加属性，消息 getProperties() 的 Advice 再将其合并到 SDK 返回的 Map。
    return instrumenterBuilder.buildProducerInstrumenter(new MessageMapSetter());
  }

  public static Instrumenter<RocketMqReceiveRequest, List<MessageView>>
      createConsumerReceiveInstrumenter(
          OpenTelemetry openTelemetry, IncludeExclude headers, boolean enabled) {
    RocketMqConsumerReceiveAttributeGetter getter = new RocketMqConsumerReceiveAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.RECEIVE;

    AttributesExtractor<RocketMqReceiveRequest, List<MessageView>> attributesExtractor =
        buildMessagingAttributesExtractor(getter, operationType, RECEIVE_OPERATION_NAME, headers);

    InstrumenterBuilder<RocketMqReceiveRequest, List<MessageView>> instrumenterBuilder =
        Instrumenter.<RocketMqReceiveRequest, List<MessageView>>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(getter, operationType, RECEIVE_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            .setEnabled(enabled)
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(new RocketMqConsumerReceiveAttributeExtractor())
            .addOperationMetrics(MessagingConsumerMetrics.get());
    // 一次 Receive 对应一批消息：公共字段放在 Span 上，消息 ID 和不同的字段放在各自的 Link 上。
    // 使用普通 Instrumenter 保留调用方父上下文，通过 Link 关联各条消息携带的生产端上下文。
    instrumenterBuilder.addAttributesExtractor(new RocketMqReceiveBatchMessageAttributeExtractor());
    instrumenterBuilder.addSpanLinksExtractor(
        new RocketMqReceiveSpanLinksExtractor(
            openTelemetry.getPropagators().getTextMapPropagator()));
    setMessagingReceiveExceptionEventExtractor(instrumenterBuilder);
    return instrumenterBuilder.buildInstrumenter(MessagingSpanKindExtractor.create(operationType));
  }

  public static Instrumenter<MessageView, ConsumeResult> createConsumerProcessInstrumenter(
      OpenTelemetry openTelemetry, IncludeExclude headers, boolean receiveInstrumentationEnabled) {
    RocketMqConsumerProcessAttributeGetter getter = new RocketMqConsumerProcessAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.PROCESS;

    AttributesExtractor<MessageView, ConsumeResult> attributesExtractor =
        buildMessagingAttributesExtractor(getter, operationType, PROCESS_OPERATION_NAME, headers);

    InstrumenterBuilder<MessageView, ConsumeResult> instrumenterBuilder =
        Instrumenter.<MessageView, ConsumeResult>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(getter, operationType, PROCESS_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            .addAttributesExtractor(attributesExtractor)
            .addAttributesExtractor(new RocketMqConsumerProcessAttributeExtractor())
            .addOperationMetrics(MessagingProcessMetrics.get())
            .setSpanStatusExtractor(
                (spanStatusBuilder, messageView, consumeResult, error) -> {
                  if (consumeResult == ConsumeResult.FAILURE) {
                    // 监听器正常返回 FAILURE 也表示处理失败，不必等到 Java 异常才标记 ERROR。
                    spanStatusBuilder.setStatus(StatusCode.ERROR);
                  } else {
                    SpanStatusExtractor.getDefault()
                        .extract(spanStatusBuilder, messageView, consumeResult, error);
                  }
                });
    if (!receiveInstrumentationEnabled) {
      // PushConsumer 关闭 Receive 遥测时，由 Process 补记已消费消息数；开启时交给 Receive 记录。
      instrumenterBuilder.addOperationMetrics(MessagingConsumerMetrics.getConsumedMessages());
    }
    setMessagingProcessExceptionEventExtractor(instrumenterBuilder);

    // 公共工厂始终关联消息携带的生产端上下文；已有有效环境 Span 时保留它作为父 Span，
    // 否则使用消息中提取的上下文作为父上下文，同时保留提取到的其他上下文值。
    return MessagingProcessInstrumenterFactory.create(
        instrumenterBuilder,
        openTelemetry.getPropagators().getTextMapPropagator(),
        new MessageMapGetter());
  }

  private static <T, R> AttributesExtractor<T, R> buildMessagingAttributesExtractor(
      MessagingAttributesGetter<T, R> getter,
      MessagingOperationType operationType,
      String operationName,
      IncludeExclude headers) {
    return MessagingAttributesExtractor.builder(getter, operationType, operationName)
        .setHeaders(headers)
        .build();
  }
}
