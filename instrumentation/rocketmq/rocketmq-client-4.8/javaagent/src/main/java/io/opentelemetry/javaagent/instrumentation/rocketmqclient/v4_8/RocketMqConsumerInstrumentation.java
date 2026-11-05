/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.consumeMessageHook;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;

// 自动为 PushConsumer 安装消费 Hook；正常的顺序消费和并发消费都通过该 Hook 包围 Listener 调用。
// 此入口不匹配 PullConsumer 或 LitePullConsumer，也不创建拉取消息的 Receive Span。
class RocketMqConsumerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.consumer.DefaultMQPushConsumer");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("start").and(takesArguments(0)), getClass().getName() + "$StartAdvice");
  }

  @SuppressWarnings("unused")
  public static class StartAdvice {
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void onEnter(
        @Advice.FieldValue(
                value = "defaultMQPushConsumerImpl",
                declaringType = DefaultMQPushConsumer.class)
            DefaultMQPushConsumerImpl defaultMqPushConsumerImpl) {
      // 在 SDK 启动消费服务之前注册，后续由 SDK 在消费线程上成对调用 Before/After。
      defaultMqPushConsumerImpl.registerConsumeMessageHook(consumeMessageHook());
    }
  }
}
