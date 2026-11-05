/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.util.List;
import org.apache.rocketmq.client.hook.SendMessageContext;

// 在 Send 创建之前保存逐条消息的创建上下文，同时供 Links、SpanKind 和批次计数使用。
// 列表既包含已有/新建的有效上下文，也可能包含 Create 被关闭或抑制后仍然无效的上下文。
final class RocketMqBatchSendSpanLinksExtractor implements SpanLinksExtractor<SendMessageContext> {
  private static final VirtualField<SendMessageContext, List<Context>> MESSAGE_CREATION_CONTEXTS =
      VirtualField.find(SendMessageContext.class, List.class, "messageCreationContexts");

  static void setContexts(SendMessageContext request, List<Context> contexts) {
    MESSAGE_CREATION_CONTEXTS.set(request, contexts);
  }

  static void clearContexts(SendMessageContext request) {
    MESSAGE_CREATION_CONTEXTS.set(request, null);
  }

  static boolean isBatchRequest(SendMessageContext request) {
    // 识别 helper 已登记的遥测请求，不等同于判断任意 Message 对象是否属于 MessageBatch。
    return MESSAGE_CREATION_CONTEXTS.get(request) != null;
  }

  static long getBatchMessageCount(SendMessageContext request) {
    List<Context> contexts = MESSAGE_CREATION_CONTEXTS.get(request);
    return contexts == null ? 0 : contexts.size();
  }

  static boolean allMessagesHaveCreationContext(SendMessageContext request) {
    // 这个判断发生在 Send.start() 期间、Send 的兜底注入之前。
    // 全部有效表示 Send 不再承担消息创建上下文的传播职责，因此可以使用 CLIENT。
    List<Context> contexts = MESSAGE_CREATION_CONTEXTS.get(request);
    if (contexts == null || contexts.isEmpty()) {
      return false;
    }
    for (Context context : contexts) {
      if (!Span.fromContext(context).getSpanContext().isValid()) {
        // SpanContext 有效性同时涉及 TraceId 和 SpanId；不要求 Span 正在记录或尚未结束。
        return false;
      }
    }
    // 每条消息均有可传播的创建上下文，可能来自独立 Create Span，也可能由应用预先提供。
    return true;
  }

  @Override
  public void extract(
      SpanLinksBuilder spanLinks, Context parentContext, SendMessageContext request) {
    List<Context> contexts = MESSAGE_CREATION_CONTEXTS.get(request);
    if (contexts == null) {
      return;
    }
    for (Context context : contexts) {
      SpanContext spanContext = Span.fromContext(context).getSpanContext();
      // Link 使用完整 SpanContext 关联消息来源，不改变 Send 的父 Span。
      // 按消息逐条添加，不对相同来源去重；无效 SpanContext 交给 SpanBuilder 按其契约忽略。
      spanLinks.addLink(spanContext);
    }
  }
}
