/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.batchSendHelper;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.common.message.Message;

// SDK 会先把批内消息的 body 和 properties 编码，再调用发送 Hook。
// 因此逐条传播字段必须在 encode() 进入前写入，否则修改内存中的消息也不会更新已编码的内容。
class RocketMqMessageBatchInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.common.message.MessageBatch");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("encode").and(takesArguments(0)), getClass().getName() + "$EncodeAdvice");
  }

  @SuppressWarnings("unused")
  public static class EncodeAdvice {
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void onEnter(@Advice.This Message batch) {
      // 使用 Message 绑定目标对象，避免在 Advice 签名中直接依赖旧版 SDK 不具备的 MessageBatch。
      // helper 只处理当前 send(Collection, ...) 已登记的状态，独立调用 encode() 不会创建 Span。
      batchSendHelper().beforeBatchEncode(batch);
    }
  }
}
