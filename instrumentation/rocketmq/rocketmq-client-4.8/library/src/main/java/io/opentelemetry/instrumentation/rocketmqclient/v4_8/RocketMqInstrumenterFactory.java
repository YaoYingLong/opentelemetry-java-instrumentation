/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingProcessExceptionEventExtractor;
import static io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingExceptionEventExtractors.setMessagingSendExceptionEventExtractor;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
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
import io.opentelemetry.instrumentation.api.instrumenter.SpanKindExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.SpanStatusExtractor;
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.SendMessageContext;

// 将 RocketMQ 请求适配到统一 Instrumenter：集中定义 Span 名称、类型、属性、Links、指标和错误规则。
// javaagent 与 library 复用这里的组件，但各自由 Hook、Advice 或代理决定何时 start/end。
class RocketMqInstrumenterFactory {

  // 该名称用于导出的 instrumentation scope，不是应用的 producerGroup 或 consumerGroup。
  private static final String INSTRUMENTATION_NAME = "io.opentelemetry.rocketmq-client-4.8";

  // messaging.operation.name values, named after the RocketMQ API operations
  private static final String SEND_OPERATION_NAME = "send";
  private static final String PROCESS_OPERATION_NAME = "process";

  // copied from MessagingIncubatingAttributes
  private static final AttributeKey<String> MESSAGING_CONSUMER_GROUP_NAME =
      AttributeKey.stringKey("messaging.consumer.group.name");
  private static final AttributeKey<String> ROCKETMQ_NAMESPACE =
      AttributeKey.stringKey("rocketmq.namespace");

  static Instrumenter<SendMessageContext, Void> createProducerInstrumenter(
      OpenTelemetry openTelemetry,
      IncludeExclude headers,
      boolean captureExperimentalSpanAttributes) {

    // 普通发送使用自身上下文作为消息创建上下文；library 的批量代理也复用这个 PRODUCER 模型。
    RocketMqProducerAttributeGetter getter = new RocketMqProducerAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.SEND;

    InstrumenterBuilder<SendMessageContext, Void> instrumenterBuilder =
        Instrumenter.<SendMessageContext, Void>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                // 生成的span名为：send 具体的topic
                MessagingSpanNameExtractor.create(getter, operationType, SEND_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            // 添加MessagingAttributesExtractor
            .addAttributesExtractor(
                buildMessagingAttributesExtractor(
                    getter, operationType, SEND_OPERATION_NAME, headers))
            .addOperationMetrics(MessagingProducerMetrics.get());
    // 添加导出rocketmq.namespace的AttributesExtractor
    instrumenterBuilder.addAttributesExtractor(producerAttributesExtractor());
    if (captureExperimentalSpanAttributes) {
      // 添加实验特性rocketmq.send_result、rocketmq.message.tag等属性导出
      instrumenterBuilder.addAttributesExtractor(
          new RocketMqProducerExperimentalAttributeExtractor());
    }
    // 为消息发送操作配置异常日志的事件名称和级别——事件名为messaging.send.exception，级别为WARN
    // 仅在 otel.semconv.exception.signal.preview 为 logs 或 logs/dup 时定制异常日志；
    // 默认的 exception Span Event、Span 错误状态和 error.type 仍由各自的公共组件处理。
    setMessagingSendExceptionEventExtractor(instrumenterBuilder);

    // start() 创建 PRODUCER Span 后调用 propagator，经 MapSetter 将其上下文写入消息属性。
    return instrumenterBuilder.buildProducerInstrumenter(new MapSetter());
  }

  static Instrumenter<SendMessageContext, Void> createBatchProducerInstrumenter(
      OpenTelemetry openTelemetry,
      IncludeExclude headers,
      boolean captureExperimentalSpanAttributes) {
    // javaagent 专用的批量 Send 模型：逐条上下文由编码前的 helper 准备，此处建立整批操作及 Links。
    RocketMqProducerAttributeGetter getter = new RocketMqProducerAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.SEND;
    InstrumenterBuilder<SendMessageContext, Void> builder =
        Instrumenter.<SendMessageContext, Void>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                // 生成的span名为：send 具体的topic
                MessagingSpanNameExtractor.create(getter, operationType, SEND_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            // 添加MessagingAttributesExtractor
            .addAttributesExtractor(
                buildMessagingAttributesExtractor(
                    getter, operationType, SEND_OPERATION_NAME, headers))
            // 相对于createProducerInstrumenter新增了RocketMqBatchSendSpanLinksExtractor
            .addSpanLinksExtractor(new RocketMqBatchSendSpanLinksExtractor())
            // 添加导出rocketmq.namespace的AttributesExtractor
            .addAttributesExtractor(producerAttributesExtractor())
            // 添加指标采集
            .addOperationMetrics(MessagingProducerMetrics.get());
    if (captureExperimentalSpanAttributes) {
      // 添加实验特性rocketmq.send_result、rocketmq.message.tag等属性导出
      builder.addAttributesExtractor(new RocketMqProducerExperimentalAttributeExtractor());
    }
    // 为消息发送操作配置异常日志的事件名称和级别——事件名为messaging.send.exception，级别为WARN
    setMessagingSendExceptionEventExtractor(builder);
    // 保留下游 Instrumenter 的传播流程，但 MapSetter 会跳过已登记批次的外层 envelope。
    // 真正注入每条消息的是 helper，防止统一的 Send 上下文覆盖各条消息已有的创建上下文。
    // 全部消息已有上下文时 Send 为 CLIENT；任意消息仍需 Send 上下文兜底时为 PRODUCER。
    return InstrumenterUtil.buildDownstreamInstrumenter(
        builder,
        new MapSetter(),
        // operationType这里固定传入的是send，这里创建的是SpanKindExtractor
        MessagingSpanKindExtractor.create(
            operationType,
            request ->
                // 当前这个 Send Span 的上下文，是否需要作为消息的“创建上下文”传播出去
                !RocketMqBatchSendSpanLinksExtractor.allMessagesHaveCreationContext(request)));
  }

  static Instrumenter<SendMessageContext, Void> createMessageCreateInstrumenter(
      OpenTelemetry openTelemetry,
      IncludeExclude headers,
      boolean batchSendMessageCreationSpansEnabled) {
    // true 让消息 ID 从 MessageClientIDSetter 读取；Create 发生在实际发送前，没有 SendResult。
    // Create 只建立单条消息的身份，不额外注册发送指标，避免把一条消息重复计入 sent.messages。
    RocketMqProducerAttributeGetter getter = new RocketMqProducerAttributeGetter(true);
    MessagingOperationType operationType = MessagingOperationType.CREATE;
    return Instrumenter.<SendMessageContext, Void>builder(
            openTelemetry,
            INSTRUMENTATION_NAME,
            // 生成的span名为：create 具体的topic
            MessagingSpanNameExtractor.create(getter, operationType, "create"))
        .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
        // 如果不调用setEnabled，其值默认是true
        .setEnabled(batchSendMessageCreationSpansEnabled)
        // 添加MessagingAttributesExtractor
        .addAttributesExtractor(
            buildMessagingAttributesExtractor(getter, operationType, "create", headers))
        .addAttributesExtractor(producerAttributesExtractor())
        // 使用普通 Instrumenter，仅生成 PRODUCER Context；注入消息的时机由批量 helper 控制。
        .buildInstrumenter(MessagingSpanKindExtractor.create(operationType));
  }

  private static AttributesExtractor<SendMessageContext, Void> producerAttributesExtractor() {
    return new AttributesExtractor<SendMessageContext, Void>() {
      @Override
      public void onStart(
          AttributesBuilder attributes, Context parentContext, SendMessageContext request) {
        // namespace 与去除前缀后的 destination 分开记录；不支持 namespace 的旧版本返回 null。
        String namespace = RocketMqNamespaceUtil.getNamespace(request);
        attributes.put(ROCKETMQ_NAMESPACE, namespace);
      }

      @Override
      public void onEnd(
          AttributesBuilder attributes,
          Context context,
          SendMessageContext request,
          @Nullable Void unused,
          @Nullable Throwable error) {}
    };
  }

  static RocketMqConsumerInstrumenter createConsumerInstrumenter(
      OpenTelemetry openTelemetry,
      IncludeExclude headers,
      boolean captureExperimentalSpanAttributes) {

    // 根据实际消费数量选择不同父上下文规则，而不是根据生产端是否曾批量发送来选择。
    return new RocketMqConsumerInstrumenter(
        createProcessInstrumenter(openTelemetry, headers, captureExperimentalSpanAttributes),
        createBatchProcessInstrumenter(openTelemetry, headers, captureExperimentalSpanAttributes));
  }

  private static Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext>
      createBatchProcessInstrumenter(
          OpenTelemetry openTelemetry,
          IncludeExclude headers,
          boolean captureExperimentalSpanAttributes) {

    RocketMqConsumerAttributeGetter getter = new RocketMqConsumerAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.PROCESS;

    // 批量 Process 只使用本地调用上下文作为父；每条消息携带的来源通过 Link 表达。
    // 公共属性留在 Span 上，消息 ID、Topic、Tag 等差异由 LinksExtractor 按消息补充。
    InstrumenterBuilder<RocketMqConsumerRequest, ConsumeMessageContext> builder =
        Instrumenter.<RocketMqConsumerRequest, ConsumeMessageContext>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(getter, operationType, PROCESS_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL)
            .addAttributesExtractor(
                buildMessagingAttributesExtractor(
                    getter, operationType, PROCESS_OPERATION_NAME, headers))
            .addAttributesExtractor(consumerAttributesExtractor())
            .addSpanLinksExtractor(
                new RocketMqBatchProcessSpanLinksExtractor(
                    openTelemetry.getPropagators().getTextMapPropagator(),
                    captureExperimentalSpanAttributes))
            .addOperationMetrics(MessagingProcessMetrics.get())
            .addOperationMetrics(MessagingConsumerMetrics.getConsumedMessages())
            .setSpanStatusExtractor(consumeStatusExtractor());
    if (captureExperimentalSpanAttributes) {
      builder.addAttributesExtractor(new RocketMqBatchProcessAttributeExtractor());
    }
    setMessagingProcessExceptionEventExtractor(builder);

    // a batch has no single message creation context that could be adopted as the span's parent,
    // so this instrumenter is built directly instead of going through
    // MessagingProcessInstrumenterFactory
    // 不采用第一条消息作为整批的父：不同消息可能来自不同 Trace，选择其中一条会歪曲因果关系。
    return builder.buildInstrumenter(SpanKindExtractor.alwaysConsumer());
  }

  private static Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext>
      createProcessInstrumenter(
          OpenTelemetry openTelemetry,
          IncludeExclude headers,
          boolean captureExperimentalSpanAttributes) {

    RocketMqConsumerAttributeGetter getter = new RocketMqConsumerAttributeGetter();
    MessagingOperationType operationType = MessagingOperationType.PROCESS;

    InstrumenterBuilder<RocketMqConsumerRequest, ConsumeMessageContext> builder =
        Instrumenter.<RocketMqConsumerRequest, ConsumeMessageContext>builder(
                openTelemetry,
                INSTRUMENTATION_NAME,
                MessagingSpanNameExtractor.create(getter, operationType, PROCESS_OPERATION_NAME))
            .setSchemaUrl(MessagingSchemaUrls.SCHEMA_URL);

    builder.addAttributesExtractor(
        buildMessagingAttributesExtractor(getter, operationType, PROCESS_OPERATION_NAME, headers));
    // 记录 Listener 处理耗时及交付应用的消息数；消费失败也属于一次已交付的处理尝试。
    builder.addOperationMetrics(MessagingProcessMetrics.get());
    builder.addOperationMetrics(MessagingConsumerMetrics.getConsumedMessages());
    builder.addAttributesExtractor(consumerAttributesExtractor());
    if (captureExperimentalSpanAttributes) {
      builder.addAttributesExtractor(new RocketMqConsumerExperimentalAttributeExtractor());
    }
    builder.setSpanStatusExtractor(consumeStatusExtractor());
    setMessagingProcessExceptionEventExtractor(builder);

    // 公共工厂同时添加消息来源 Link 和父上下文定制：有当前 Span 时保留它作为父，
    // 没有当前 Span 时才采用消息携带的上下文作为父；即使采用了消息父上下文，也保留来源 Link。
    return MessagingProcessInstrumenterFactory.create(
        builder,
        openTelemetry.getPropagators().getTextMapPropagator(),
        new TextMapExtractAdapter());
  }

  private static AttributesExtractor<RocketMqConsumerRequest, ConsumeMessageContext>
      consumerAttributesExtractor() {
    return new AttributesExtractor<RocketMqConsumerRequest, ConsumeMessageContext>() {
      @Override
      public void onStart(
          AttributesBuilder attributes, Context parentContext, RocketMqConsumerRequest request) {
        attributes.put(MESSAGING_CONSUMER_GROUP_NAME, request.getConsumerGroup());
        attributes.put(ROCKETMQ_NAMESPACE, request.getNamespace());
      }

      @Override
      public void onEnd(
          AttributesBuilder attributes,
          Context context,
          RocketMqConsumerRequest request,
          @Nullable ConsumeMessageContext response,
          @Nullable Throwable error) {}
    };
  }

  private static SpanStatusExtractor<RocketMqConsumerRequest, ConsumeMessageContext>
      consumeStatusExtractor() {
    return (spanStatusBuilder, request, response, error) -> {
      // the consume return type, and not just the consume status, decides whether the operation
      // failed, so that the span status stays consistent with the reported error.type
      if (RocketMqConsumerAttributeGetter.getResponseErrorType(response) != null) {
        // SDK 可能报告 TIME_OUT，但最终 status 仍为成功；与 error.type 使用同一分类以免矛盾。
        spanStatusBuilder.setStatus(StatusCode.ERROR);
      } else {
        SpanStatusExtractor.getDefault().extract(spanStatusBuilder, request, response, error);
      }
    };
  }

  private static <T, R> AttributesExtractor<T, R> buildMessagingAttributesExtractor(
      MessagingAttributesGetter<T, R> getter,
      MessagingOperationType operationType,
      String operationName,
      IncludeExclude headers) {
    // 公共提取器在开始时记录系统、目标和操作，在结束时记录消息 ID、数量、error.type 和选中 Header。
    // headers 仅控制属性采集，与消息中的 trace 上下文注入和提取无关。
    return MessagingAttributesExtractor.builder(getter, operationType, operationName)
        .setHeaders(headers)
        .build();
  }

  private RocketMqInstrumenterFactory() {}
}
