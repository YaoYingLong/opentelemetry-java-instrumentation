/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.propagation.TextMapGetter;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.Message;

// 直接从消息 properties 提取传播字段：生产侧读取已有创建上下文，批量消费侧逐条构造来源 Link。
// 使用 Message 基类可同时接收发送消息和消费侧的 MessageExt；采用什么基底 Context 由调用方决定。
final class MessageExtractAdapter implements TextMapGetter<Message> {

  @Override
  public Iterable<String> keys(Message carrier) {
    return carrier.getProperties().keySet();
  }

  @Nullable
  @Override
  public String get(@Nullable Message carrier, String key) {
    return carrier == null ? null : carrier.getProperties().get(key);
  }
}
