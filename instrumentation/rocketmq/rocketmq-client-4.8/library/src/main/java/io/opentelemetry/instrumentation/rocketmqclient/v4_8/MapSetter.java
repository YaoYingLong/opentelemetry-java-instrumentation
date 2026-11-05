/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.propagation.TextMapSetter;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.common.message.Message;

// 将 propagator 给出的传播字段写入发送请求中的消息 properties，字段名由实际 propagator 决定。
// 这里负责上下文传播；是否把相同字段采集成 Span 属性由 Header 选择器另行控制。
final class MapSetter implements TextMapSetter<SendMessageContext> {

  @Override
  public void set(@Nullable SendMessageContext carrier, String key, String value) {
    if (carrier == null) {
      return;
    }
    Message message = carrier.getMessage();
    if (message == null || RocketMqBatchSendSpanLinksExtractor.isBatchRequest(carrier)) {
      // 已登记的 javaagent 批次由 helper 在编码前逐条注入，此处跳过外层消息以保留不同的创建上下文。
      // isBatchRequest 判断的是状态登记，不是所有 MessageBatch；library 代理的请求有自己的处理路径。
      return;
    }
    message.getProperties().put(key, value);
  }
}
