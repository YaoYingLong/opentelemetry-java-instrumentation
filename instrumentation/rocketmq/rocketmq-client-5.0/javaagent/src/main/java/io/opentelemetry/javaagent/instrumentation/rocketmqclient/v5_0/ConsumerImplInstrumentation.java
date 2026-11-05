/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import apache.rocketmq.v2.ReceiveMessageRequest;
import io.opentelemetry.instrumentation.api.internal.Timer;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.java.impl.consumer.ReceiveMessageResult;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.ListenableFuture;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.MoreExecutors;

/**
 * 观察 ConsumerImpl 面向单个队列的异步接收，用于记录 PushConsumer 的 Receive 操作。
 *
 * <p>入口先计时，Future 完成后才知道是否收到消息、收到多少条，再由回调补记 Span 和指标。 SimpleConsumer 的接收由更外层的 receive0
 * 统一记录，因此在这里跳过。
 */
final class ConsumerImplInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.java.impl.consumer.ConsumerImpl");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("receiveMessage")
            .and(takesArgument(0, named("apache.rocketmq.v2.ReceiveMessageRequest"))),
        getClass().getName() + "$ReceiveMessageAdvice");
  }

  @SuppressWarnings("unused")
  public static class ReceiveMessageAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static Timer onStart(@Advice.This Object consumer) {
      // 同一次 SimpleConsumer 拉取已经由外层插桩负责，避免底层队列调用重复记录 Receive。
      if (SimpleConsumerReceiveOperation.handlesReceive(consumer)) {
        return null;
      }
      return Timer.start();
    }

    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static void onExit(
        @Advice.Argument(0) ReceiveMessageRequest request,
        @Advice.Enter Timer timer,
        @Advice.Return ListenableFuture<ReceiveMessageResult> future) {
      if (timer == null) {
        return;
      }
      ReceiveSpanFinishingCallback spanFinishingCallback =
          new ReceiveSpanFinishingCallback(request, timer);
      // 方法返回仅代表拿到 Future；网络接收耗时和错误必须等 Future 的终态才能确定。
      Futures.addCallback(future, spanFinishingCallback, MoreExecutors.directExecutor());
    }
  }
}
