/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.internal.MessagingConfig;
import io.opentelemetry.instrumentation.api.internal.ScopedThreadValue;
import io.opentelemetry.instrumentation.rocketmqclient.v4_8.RocketMqBatchSendHelper;
import io.opentelemetry.instrumentation.rocketmqclient.v4_8.RocketMqBatchSendHelper.BatchSendState;
import io.opentelemetry.instrumentation.rocketmqclient.v4_8.RocketMqTelemetry;
import io.opentelemetry.javaagent.bootstrap.internal.ExperimentalConfig;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.SendMessageHook;

// 汇集自动插桩使用的配置、Instrumenter 与 Hook；单次发送/消费的状态保存在各自请求上。
public class RocketMqSingletons {

  // 把选中的消息属性记录到Span上，便于按业务属性查询和排查消息链路
  // 选择器在初始化时读取；默认不采集 Header，这与 propagator 注入 traceparent 等字段相互独立。
  private static final IncludeExclude headers = ExperimentalConfig.get().getMessagingHeaders();
  // 开启后会添加RocketMqConsumerExperimentalAttributeExtractor和RocketMqProducerExperimentalAttributeExtractor
  // 批量消费使用单独的公共 Tag 提取器，不把第一条消息的队列和 Broker 属性当成整批属性。
  private static final boolean captureExperimentalSpanAttributes =
      DeclarativeConfigUtil.getInstrumentationConfig(GlobalOpenTelemetry.get(), "rocketmq_client")
          .getBoolean("experimental_span_attributes/development", false);

  private static final RocketMqTelemetry telemetry =
      RocketMqTelemetry.builder(GlobalOpenTelemetry.get())
          .setHeaders(headers)
          .setCaptureExperimentalSpanAttributes(captureExperimentalSpanAttributes)
          .build();

  // 连接 send(Collection) Advice 与同线程中的 encode() Advice；异步发送阶段靠消息上的状态关联。
  // set/restore 保存并恢复外层状态，因此嵌套批量发送退出后仍能继续处理原来的批次。
  private static final ScopedThreadValue<BatchSendState> currentBatchSendState =
      new ScopedThreadValue<>();

  private static final RocketMqBatchSendHelper batchSendHelper =
      new RocketMqBatchSendHelper(
          GlobalOpenTelemetry.get(),
          headers,
          captureExperimentalSpanAttributes,
          // 模块级开关优先于 common.messaging 开关，均未设置时默认创建逐条 Create Span。
          MessagingConfig.isBatchSendMessageCreationSpansEnabled(
              GlobalOpenTelemetry.get(), "rocketmq_client"));

  private static final ConsumeMessageHook consumeMessageHook = telemetry.createConsumeMessageHook();

  // 发送 Hook 先识别已登记的批次；非批次请求再委托给普通发送 Hook，避免重复创建 Send Span。
  private static final SendMessageHook sendMessageHook = batchSendHelper.createSendMessageHook();

  public static RocketMqBatchSendHelper batchSendHelper() {
    return batchSendHelper;
  }

  public static ConsumeMessageHook consumeMessageHook() {
    return consumeMessageHook;
  }

  public static ScopedThreadValue<BatchSendState> currentBatchSendState() {
    return currentBatchSendState;
  }

  public static SendMessageHook sendMessageHook() {
    return sendMessageHook;
  }

  private RocketMqSingletons() {}
}
