/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;

/**
 * Entrypoint for instrumenting RocketMq producers or consumers.
 *
 * <p>手动接入入口：生产者通过 instrument() 返回的代理发送，消费者向 SDK 注册消费 Hook。 javaagent 也复用消费组件，但其逐条 Create 和批量 Send
 * 生命周期由独立 helper 管理。
 */
public final class RocketMqTelemetry {
  private final RocketMqConsumerInstrumenter rocketMqConsumerInstrumenter;
  private final Instrumenter<SendMessageContext, Void> rocketMqProducerInstrumenter;
  private final TextMapPropagator propagator;

  /** Returns a new {@link RocketMqTelemetry} configured with the given {@link OpenTelemetry}. */
  public static RocketMqTelemetry create(OpenTelemetry openTelemetry) {
    return builder(openTelemetry).build();
  }

  /**
   * Returns a new {@link RocketMqTelemetryBuilder} configured with the given {@link OpenTelemetry}.
   */
  public static RocketMqTelemetryBuilder builder(OpenTelemetry openTelemetry) {
    return new RocketMqTelemetryBuilder(openTelemetry);
  }

  RocketMqTelemetry(
      OpenTelemetry openTelemetry,
      IncludeExclude headers,
      boolean captureExperimentalSpanAttributes) {
    // 固化本实例配置；手动 library 接入由 builder 传参，不依赖 javaagent 的单例配置读取。
    rocketMqConsumerInstrumenter =
        RocketMqInstrumenterFactory.createConsumerInstrumenter(
            openTelemetry, headers, captureExperimentalSpanAttributes);
    // 目前的实现中已经没有再使用了，仅仅测试在使用
    rocketMqProducerInstrumenter =
        RocketMqInstrumenterFactory.createProducerInstrumenter(
            openTelemetry, headers, captureExperimentalSpanAttributes);
    propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  /**
   * Returns a new {@link ConsumeMessageHook} for use with methods like {@link
   * org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl#registerConsumeMessageHook(ConsumeMessageHook)}.
   */
  public ConsumeMessageHook createConsumeMessageHook() {
    // 这里只创建 Hook，调用方负责注册；自动接入时由 ConsumerInstrumentation 在 start() 前注册。
    return new TracingConsumeMessageHookImpl(rocketMqConsumerInstrumenter);
  }

  /**
   * Returns an instrumented producer that emits telemetry and propagates context for single-message
   * and batch sends.
   *
   * <p>Configure the supplied producer before calling this method, and send messages through the
   * returned producer. It registers its send hook automatically. Use the collection-based {@code
   * send} overloads for batches so context can be injected before encoding.
   */
  public MQProducer instrument(DefaultMQProducer producer) {
    // 原 producer 保留连接与生命周期所有权；调用方需使用返回的代理，批量编码前的注入才能生效。
    return RocketMqProducerWrapper.wrap(producer, rocketMqProducerInstrumenter, propagator);
  }

  /**
   * Returns a new {@link SendMessageHook} for use with methods like {@link
   * org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#registerSendMessageHook(SendMessageHook)}.
   *
   * @deprecated Use {@link #instrument(DefaultMQProducer)} instead. May be removed in the next
   *     minor release.
   */
  @Deprecated // may be removed in the next minor release
  public SendMessageHook createSendMessageHook() {
    // 旧入口只有底层 Hook，无法提供批量编码前的处理，因此推荐改用 instrument(producer)。
    return RocketMqProducerWrapper.createSendMessageHook(rocketMqProducerInstrumenter);
  }
}
