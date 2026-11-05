/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.asm.Advice.AssignReturned.ToArguments.ToArgument;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.apis.consumer.MessageListener;

/**
 * 在 SDK 创建 ConsumeService 时替换 MessageListener，让实际业务消费进入统一的 Process 包装器。
 *
 * <p>兼容监听器处于第 2 或第 3 个参数的构造签名；只替换监听器参数，不接管 SDK 的调度与重试。
 */
final class ConsumeServiceInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.java.impl.consumer.ConsumeService");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isConstructor()
            .and(
                isPublic()
                    .and(
                        takesArgument(
                            1, named("org.apache.rocketmq.client.apis.consumer.MessageListener")))),
        getClass().getName() + "$Constructor1Advice");

    transformer.applyAdviceToMethod(
        isConstructor()
            .and(
                isPublic()
                    .and(
                        takesArgument(
                            2, named("org.apache.rocketmq.client.apis.consumer.MessageListener")))),
        getClass().getName() + "$Constructor2Advice");
  }

  @SuppressWarnings("unused")
  public static class Constructor1Advice {
    @AssignReturned.ToArguments(@ToArgument(1))
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static MessageListener onEnter(@Advice.Argument(1) MessageListener messageListener) {
      // 返回值通过 AssignReturned 写回构造参数，SDK 后续调用保存的监听器时就会进入包装器。
      return MessageListenerWrapper.wrapIfNeeded(messageListener);
    }
  }

  @SuppressWarnings("unused")
  public static class Constructor2Advice {
    @AssignReturned.ToArguments(@ToArgument(2))
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static MessageListener onEnter(@Advice.Argument(2) MessageListener messageListener) {
      // 另一种构造签名复用同一包装逻辑，wrapIfNeeded 避免已有包装器被再次包装。
      return MessageListenerWrapper.wrapIfNeeded(messageListener);
    }
  }
}
