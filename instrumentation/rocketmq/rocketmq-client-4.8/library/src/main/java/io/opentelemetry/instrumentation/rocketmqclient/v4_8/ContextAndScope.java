/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import com.google.auto.value.AutoValue;
import io.opentelemetry.context.Scope;

// 消费 Hook 在同一线程成对调用，因此可以把线程绑定的 Scope 与操作状态一起保存。
// close() 只恢复线程上下文；真正结束 Process Span 的职责仍在 RocketMqConsumerInstrumenter。
@AutoValue
abstract class ContextAndScope {

  static ContextAndScope create(
      RocketMqConsumerInstrumenter.ConsumerContext consumerContext, Scope scope) {
    return new AutoValue_ContextAndScope(consumerContext, scope);
  }

  abstract RocketMqConsumerInstrumenter.ConsumerContext getConsumerContext();

  abstract Scope getScope();

  void close() {
    getScope().close();
  }
}
