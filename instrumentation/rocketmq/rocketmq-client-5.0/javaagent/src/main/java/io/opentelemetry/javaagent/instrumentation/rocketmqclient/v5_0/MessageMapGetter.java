/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.context.propagation.TextMapGetter;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.message.MessageView;

/** 从收到的消息属性中读取传播字段，供 Receive 的 Link 提取和 Process 的上下文恢复共同使用。 */
class MessageMapGetter implements TextMapGetter<MessageView> {

  @Override
  public Iterable<String> keys(MessageView carrier) {
    return carrier.getProperties().keySet();
  }

  @Nullable
  @Override
  public String get(@Nullable MessageView carrier, String key) {
    return carrier == null ? null : carrier.getProperties().get(key);
  }
}
