/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.List;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.ListenableFuture;

/**
 * 在 SimpleConsumer 同步 receive 和异步 receiveAsync 共用的 receive0 入口记录一次完整拉取。
 *
 * <p>入口捕获消费组、调用方上下文和开始时间；返回时替换内部 Future，由完成路径记录结果。 这里只观察 SDK 拉取，不自动为应用拿到消息后的任意业务代码创建 Process Span。
 */
final class SimpleConsumerImplInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.java.impl.consumer.SimpleConsumerImpl");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(named("receive0"), getClass().getName() + "$ReceiveAdvice");
  }

  @SuppressWarnings("unused")
  public static class ReceiveAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static SimpleConsumerReceiveOperation onEnter(
        @Advice.This SimpleConsumer simpleConsumer) {
      return SimpleConsumerReceiveOperation.start(simpleConsumer);
    }

    @AssignReturned.ToReturned
    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static ListenableFuture<List<MessageView>> onExit(
        @Advice.Enter SimpleConsumerReceiveOperation operation,
        @Advice.Return ListenableFuture<List<MessageView>> future) {
      // Enter Advice 发生被抑制的异常时 operation 可能为 null，此时保持 SDK 原始返回值。
      return operation == null ? future : operation.wrap(future);
    }
  }
}
