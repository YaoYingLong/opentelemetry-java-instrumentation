/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;

// 一次消费 Hook 对应一次 Listener 调用，记录消息交给应用后的 Process 操作。
// Span 不包含 Broker 排队或消息拉取；Listener 自行异步提交的工作也不会使此 Span 自动延长。
final class TracingConsumeMessageHookImpl implements ConsumeMessageHook {

  // 保存本次操作的请求、Context 和 Scope，After 使用原始状态完成操作并恢复消费线程。
  private static final VirtualField<ConsumeMessageContext, ContextAndScope>
      CONTEXT_AND_SCOPE_FIELD =
          VirtualField.find(ConsumeMessageContext.class, ContextAndScope.class);

  private final RocketMqConsumerInstrumenter instrumenter;

  TracingConsumeMessageHookImpl(RocketMqConsumerInstrumenter instrumenter) {
    this.instrumenter = instrumenter;
  }

  @Override
  public String hookName() {
    return "OpenTelemetryConsumeMessageTraceHook";
  }

  @Override
  public void consumeMessageBefore(ConsumeMessageContext context) {
    if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
      return;
    }
    Context parentContext = Context.current();
    // msgList、consumerGroup 和 namespace 由 SDK 构造 ConsumeMessageContext 时提供。
    // 正常 Push 消费进入 Hook 前，SDK 已处理消息 Topic 上的重试与 namespace 前缀。
    RocketMqConsumerInstrumenter.ConsumerContext consumerContext =
        instrumenter.start(
            parentContext,
            context.getMsgList(),
            context.getConsumerGroup(),
            RocketMqNamespaceUtil.getNamespace(context));

    // it's safe to store the scope in the rocketMq message context, both before() and after()
    // methods are always called from the same thread; see:
    // - ConsumeMessageConcurrentlyService$ConsumeRequest#run()
    // - ConsumeMessageOrderlyService$ConsumeRequest#run()
    if (consumerContext != null) {
      Context newContext = consumerContext.getContext();
      // Process 成为 Listener 执行期间的当前 Span，内部 JDBC、HTTP 等操作可以自然继承它。
      CONTEXT_AND_SCOPE_FIELD.set(
          context, ContextAndScope.create(consumerContext, newContext.makeCurrent()));
    }
  }

  @Override
  public void consumeMessageAfter(ConsumeMessageContext context) {
    if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
      return;
    }
    ContextAndScope contextAndScope = CONTEXT_AND_SCOPE_FIELD.get(context);
    if (contextAndScope != null) {
      // 先恢复线程上下文，再提取消费结果并结束 Span；避免完成遥测时仍占用业务的当前 Scope。
      contextAndScope.close();
      instrumenter.end(contextAndScope.getConsumerContext(), context);
    }
  }
}
