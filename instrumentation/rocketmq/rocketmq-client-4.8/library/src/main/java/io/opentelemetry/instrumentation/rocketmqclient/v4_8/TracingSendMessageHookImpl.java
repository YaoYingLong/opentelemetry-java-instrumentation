/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;

// 普通发送通过 SDK Hook 创建、结束 Send Span；library 代理已接管的批次只在这里补充结果。
// Span 从底层 Before Hook 开始，不覆盖进入 Hook 前的全部校验、路由查找或异步排队。
final class TracingSendMessageHookImpl implements SendMessageHook {

  // 同一个 SDK 请求的 Before/After 可能跨线程，不能在 After 中重新使用 Context.current()。
  private static final VirtualField<SendMessageContext, Context> CONTEXT_FIELD =
      VirtualField.find(SendMessageContext.class, Context.class);

  private final Instrumenter<SendMessageContext, Void> instrumenter;

  TracingSendMessageHookImpl(Instrumenter<SendMessageContext, Void> instrumenter) {
    this.instrumenter = instrumenter;
  }

  @Override
  public String hookName() {
    return "OpenTelemetrySendMessageTraceHook";
  }

  @Override
  public void sendMessageBefore(SendMessageContext context) {
    if (context == null) {
      return;
    }
    RocketMqProducerWrapper.SendState state = RocketMqProducerWrapper.getState(context);
    if (state != null) {
      // library 批量代理已经在编码前启动了操作，避免再为外层 MessageBatch 创建一个 Send Span。
      state.copyFrom(context);
      return;
    }
    Context parentContext = Context.current();
    // shouldStart 统一处理 Instrumenter 启用状态及已有上下文中的 Span 抑制规则。
    if (!instrumenter.shouldStart(parentContext, context)) {
      return;
    }
    // Producer Instrumenter 的 start() 内部通过 MapSetter 注入消息 properties。
    // 此处只保存新 Context，没有 makeCurrent()，所以发送 Span 不会在这里成为当前线程的 Span。
    CONTEXT_FIELD.set(context, instrumenter.start(parentContext, context));
  }

  @Override
  public void sendMessageAfter(SendMessageContext context) {
    if (context == null) {
      return;
    }
    RocketMqProducerWrapper.SendState state = RocketMqProducerWrapper.getState(context);
    if (state != null) {
      state.copyFrom(context);
      return;
    }
    Context otelContext = CONTEXT_FIELD.get(context);
    // ASYNC 提交阶段可能先调用一次没有结果的 After，不能提前结束；有结果或异常才表示完成。
    // ONEWAY 不等待 Broker 应答，因此即使没有 SendResult，也在 After 结束发送操作。
    if (otelContext != null
        && (context.getSendResult() != null
            || context.getException() != null
            || CommunicationMode.ONEWAY == context.getCommunicationMode())) {
      // SendResult 本身保存在 request 中，RESPONSE 类型为 Void；异常通过最后一个参数参与判错和记录。
      instrumenter.end(otelContext, context, null, context.getException());
    }
  }
}
