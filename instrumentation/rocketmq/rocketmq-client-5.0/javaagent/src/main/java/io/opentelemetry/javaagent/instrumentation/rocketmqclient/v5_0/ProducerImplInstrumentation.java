/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0.RocketMqSingletons.producerInstrumenter;
import static net.bytebuddy.matcher.ElementMatchers.isPrivate;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.impl.producer.SendReceiptImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.MoreExecutors;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.SettableFuture;

/**
 * 在 ProducerImpl 的内部发送入口创建消息级 Send Span，并在发送 Future 完成时结束。
 *
 * <p>内部入口可能已切换到 SDK 线程，所以父上下文优先使用消息构造时保存的值；公开 sendAsync 返回的 CompletableFuture 则单独包装，用于恢复应用完成回调的上下文。
 */
final class ProducerImplInstrumentation implements TypeInstrumentation {

  /** Match the implementation of RocketMQ producer. */
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.java.impl.producer.ProducerImpl");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    // 选中持有整批完成 Future、候选队列和待发消息的 send0 重载；参数下标从 0 开始。
    // SDK 的重试也可能重新进入此方法，Span 的结束仍以这里传入的完成 Future 为准。
    transformer.applyAdviceToMethod(
        named("send0")
            .and(isPrivate())
            .and(
                takesArgument(
                    0,
                    named(
                        "org.apache.rocketmq.shaded.com.google.common.util.concurrent.SettableFuture")))
            .and(takesArgument(3, List.class))
            .and(takesArgument(4, List.class)),
        getClass().getName() + "$SendAdvice");

    transformer.applyAdviceToMethod(named("sendAsync"), getClass().getName() + "$SendAsyncAdvice");
  }

  @SuppressWarnings("unused")
  public static class SendAdvice {
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void onEnter(
        @Advice.Argument(0) SettableFuture<List<SendReceiptImpl>> future0,
        @Advice.Argument(3) List<MessageQueueImpl> messageQueues,
        @Advice.Argument(4) List<PublishingMessageImpl> messages) {
      Instrumenter<PublishingMessageImpl, SendReceiptImpl> instrumenter = producerInstrumenter();
      int count = messages.size();
      // SDK 返回整批回执，插桩按消息逐条创建 Span，因此把完成信号按相同下标分发。
      List<SettableFuture<SendReceiptImpl>> futures = FutureConverter.convert(future0, count);
      // namespace 来自候选队列的 Topic Resource，不是 Broker 地址或消息 tag。
      @Nullable
      String namespace =
          messageQueues.isEmpty()
              ? null
              : StringUtils.emptyToNull(messageQueues.get(0).getTopicResource().getNamespace());
      for (int i = 0; i < count; i++) {
        PublishingMessageImpl message = messages.get(i);
        VirtualFieldStore.setNamespaceByMessage(message, namespace);

        // 构造 PublishingMessageImpl 时仍在应用调用线程，保存的 Context 可以跨路由查询等异步步骤传递。
        // 在PublishingMessageImplInstrumentation中被设置
        Context parentContext = VirtualFieldStore.getContextByMessage(message);
        if (parentContext == null) {
          parentContext = Context.current();
        }

        Span span = Span.fromContext(parentContext);
        if (!span.getSpanContext().isValid()) {
          // 当前实现仅在保存的上下文含有效 Span 时优先沿用，否则退回发送入口的环境上下文。
          parentContext = Context.current();
        }

        SettableFuture<SendReceiptImpl> future = futures.get(i);
        if (!instrumenter.shouldStart(parentContext, message)) {
          continue;
        }
        Context context = instrumenter.start(parentContext, message);
        // start 同时完成传播字段注入；这里不打开线程 Scope，Span 生命周期由回执回调持有。
        // directExecutor 在完成线程执行；若 Future 已完成，也可能在注册回调时立即执行。
        Futures.addCallback(
            future,
            // 在SendSpanFinishingCallback的onSuccess和onFailure中结束span
            new SendSpanFinishingCallback(instrumenter, context, message),
            MoreExecutors.directExecutor());
      }
    }
  }

  @SuppressWarnings("unused")
  public static class SendAsyncAdvice {
    @AssignReturned.ToReturned
    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static CompletableFuture<SendReceipt> onExit(
        @Advice.Return CompletableFuture<SendReceipt> future) {
      // 替换应用拿到的返回值，让因该 Future 完成而同步触发的后续动作看到应用调用方上下文。
      return CompletableFutureWrapper.wrap(future);
    }
  }
}
