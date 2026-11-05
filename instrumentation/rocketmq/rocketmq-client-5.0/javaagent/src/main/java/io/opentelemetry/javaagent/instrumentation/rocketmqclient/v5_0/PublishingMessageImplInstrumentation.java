/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;

import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Map;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.java.message.MessageImpl;
import org.apache.rocketmq.client.java.message.PublishingMessageImpl;

/**
 * 将应用线程的 Context 附着到 SDK 的待发送消息，并在读取消息属性时补入传播字段。
 *
 * <p>同时匹配 PublishingMessageImpl 与 MessageImpl，以覆盖 getProperties 在消息实现或父类上的声明。
 * 真正合并属性前仍检查消息类型，只对待发送消息生效。
 */
final class PublishingMessageImplInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return namedOneOf(
        "org.apache.rocketmq.client.java.message.PublishingMessageImpl",
        "org.apache.rocketmq.client.java.message.MessageImpl");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isConstructor()
            .and(isPublic())
            .and(takesArgument(0, named("org.apache.rocketmq.client.apis.message.Message")))
            .and(
                takesArgument(
                    1,
                    // The constructor declares ProducerSettings in 5.0.0 and
                    // PublishingSettings in 5.0.1+.
                    namedOneOf(
                        "org.apache.rocketmq.client.java.impl.producer.ProducerSettings",
                        "org.apache.rocketmq.client.java.impl.producer.PublishingSettings")))
            .and(takesArgument(2, boolean.class)),
        getClass().getName() + "$ConstructorAdvice");
    transformer.applyAdviceToMethod(
        named("getProperties").and(isPublic()), getClass().getName() + "$GetPropertiesAdvice");
  }

  @SuppressWarnings("unused")
  public static class ConstructorAdvice {
    /**
     * The constructor of {@link PublishingMessageImpl} is always called in the same thread that
     * user invoke {@link Producer#send(Message)}/{@link Producer#sendAsync(Message)}/{@link
     * Producer#send(Message, Transaction)}. Store the {@link Context} here and fetch it in {@link
     * ProducerImplInstrumentation}.
     */
    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static void onExit(@Advice.This PublishingMessageImpl message) {
      // 只保存父上下文，Send Span 要等内部发送入口拿到路由信息后才创建。
      VirtualFieldStore.setContextByMessage(message, Context.current());
    }
  }

  @SuppressWarnings("unused")
  public static class GetPropertiesAdvice {
    /** Update the message properties to propagate context recorded by {@link MessageMapSetter}. */
    @AssignReturned.ToReturned
    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static Map<String, String> onExit(
        @Advice.This MessageImpl messageImpl, @Advice.Return Map<String, String> properties) {
      if (!(messageImpl instanceof PublishingMessageImpl)) {
        return properties;
      }
      PublishingMessageImpl message = (PublishingMessageImpl) messageImpl;
      Map<String, String> extraProperties = VirtualFieldStore.getExtraPropertiesByMessage(message);
      if (extraProperties != null) {
        // SDK 的属性读取结果还会用于构造 Protobuf userProperties；在此合并才能随消息发出。
        // 同名字段由传播器本次写入的值覆盖，附加属性本身保存在 VirtualField 中。
        properties.putAll(extraProperties);
      }
      return properties;
    }
  }
}
