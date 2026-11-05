/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.javaagent.bootstrap.internal.ExperimentalConfig;
import java.util.List;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/** 保存本模块复用的 Instrumenter，在类初始化时统一读取遥测配置并完成装配。 */
public class RocketMqSingletons {

  private static final Instrumenter<PublishingMessageImpl, SendReceiptImpl> producerInstrumenter;
  private static final Instrumenter<RocketMqReceiveRequest, List<MessageView>>
      consumerReceiveInstrumenter;
  private static final Instrumenter<RocketMqReceiveRequest, List<MessageView>>
      simpleConsumerReceiveInstrumenter;
  private static final Instrumenter<MessageView, ConsumeResult> consumerProcessInstrumenter;

  static {
    OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    // headers 控制哪些消息属性被采集为遥测属性，与传播器注入 traceparent 等字段是两件事。
    IncludeExclude messagingHeaders = ExperimentalConfig.get().getMessagingHeaders();
    boolean receiveInstrumentationEnabled =
        ExperimentalConfig.get().messagingReceiveInstrumentationEnabled();

    producerInstrumenter =
        RocketMqInstrumenterFactory.createProducerInstrumenter(openTelemetry, messagingHeaders);
    consumerReceiveInstrumenter =
        RocketMqInstrumenterFactory.createConsumerReceiveInstrumenter(
            openTelemetry, messagingHeaders, receiveInstrumentationEnabled);
    // SimpleConsumer 没有本模块可包装的业务监听器，因此即使关闭实验性 Receive 遥测，
    // 仍启用其拉取遥测；shouldStart 仍会遵守 Instrumenter 的正常抑制规则。
    simpleConsumerReceiveInstrumenter =
        RocketMqInstrumenterFactory.createConsumerReceiveInstrumenter(
            openTelemetry, messagingHeaders, true);
    consumerProcessInstrumenter =
        RocketMqInstrumenterFactory.createConsumerProcessInstrumenter(
            openTelemetry, messagingHeaders, receiveInstrumentationEnabled);
  }

  public static Instrumenter<PublishingMessageImpl, SendReceiptImpl> producerInstrumenter() {
    return producerInstrumenter;
  }

  public static Instrumenter<RocketMqReceiveRequest, List<MessageView>>
      consumerReceiveInstrumenter() {
    return consumerReceiveInstrumenter;
  }

  public static Instrumenter<RocketMqReceiveRequest, List<MessageView>>
      simpleConsumerReceiveInstrumenter() {
    return simpleConsumerReceiveInstrumenter;
  }

  public static Instrumenter<MessageView, ConsumeResult> consumerProcessInstrumenter() {
    return consumerProcessInstrumenter;
  }

  private RocketMqSingletons() {}
}
