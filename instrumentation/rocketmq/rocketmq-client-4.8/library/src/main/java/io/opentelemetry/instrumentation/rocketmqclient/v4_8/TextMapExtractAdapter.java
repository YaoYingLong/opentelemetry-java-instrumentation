/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.propagation.TextMapGetter;
import javax.annotation.Nullable;

// 单条 Process 使用的请求级适配器，由公共消费工厂用于消息来源 Link 和父上下文选择。
// 批量消费不走此适配器：一个批次不能只用第一条消息的传播字段代表全部来源。
final class TextMapExtractAdapter implements TextMapGetter<RocketMqConsumerRequest> {

  @Override
  public Iterable<String> keys(RocketMqConsumerRequest carrier) {
    return carrier.getMessage().getProperties().keySet();
  }

  @Nullable
  @Override
  public String get(@Nullable RocketMqConsumerRequest carrier, String key) {
    return carrier == null ? null : carrier.getMessage().getProperties().get(key);
  }
}
