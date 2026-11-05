/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.batchSendHelper;
import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.currentBatchSendState;
import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.sendMessageHook;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.not;
import static net.bytebuddy.matcher.ElementMatchers.takesArgument;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.opentelemetry.instrumentation.rocketmqclient.v4_8.RocketMqBatchSendHelper.BatchSendState;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.asm.Advice.AssignReturned.ToArguments.ToArgument;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;

// 普通发送由 SDK 的 SendMessageHook 跟踪；集合发送还需要提前捕获调用方上下文，
// 让 MessageBatch.encode() 的 Advice 能在序列化之前处理批内每条消息。
class RocketMqProducerInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.rocketmq.client.producer.DefaultMQProducer");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    // 这里跟老版本一样没有变化
    // 在 start() 进入时注册 Hook，后续底层发送会通过 SDK 原有扩展点触发遥测。
    transformer.applyAdviceToMethod(
        named("start").and(takesArguments(0)), getClass().getName() + "$StartAdvice");
    // 匹配所有第一个参数是Collection的send方法
    ElementMatcher.Junction<MethodDescription> batchSendMethods =
        named("send").and(isPublic()).and(takesArgument(0, named("java.util.Collection")));
    // 匹配第二个参数的声明类型为SendCallback的方法
    // 参数索引从 0 开始；这里检查方法签名，调用时 callback 为 null 仍然匹配。
    ElementMatcher.Junction<MethodDescription> callbackAtArgumentOne =
        takesArgument(1, named("org.apache.rocketmq.client.producer.SendCallback"));
    // 匹配第三个参数的声明类型为SendCallback的方法
    // 对应 send(Collection, MessageQueue, SendCallback, ...) 这一组重载。
    ElementMatcher.Junction<MethodDescription> callbackAtArgumentTwo =
        takesArgument(2, named("org.apache.rocketmq.client.producer.SendCallback"));
    // 匹配第一个参数是Collection的send方法，且第二个和第三个参数的声明类型不为SendCallback的方法
    transformer.applyAdviceToMethod(
        batchSendMethods.and(not(callbackAtArgumentOne.or(callbackAtArgumentTwo))),
        getClass().getName() + "$BatchSendAdvice");
    // 匹配第一个参数是Collection的send方法，且第二个参数的声明类型为SendCallback的方法
    transformer.applyAdviceToMethod(
        batchSendMethods.and(callbackAtArgumentOne),
        getClass().getName() + "$AsyncBatchSendArgumentOneAdvice");
    // 匹配第一个参数是Collection的send方法，且第三个参数的声明类型为SendCallback的方法
    transformer.applyAdviceToMethod(
        batchSendMethods.and(callbackAtArgumentTwo),
        getClass().getName() + "$AsyncBatchSendArgumentTwoAdvice");
  }

  @SuppressWarnings("unused")
  public static class StartAdvice {
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void onEnter(
        @Advice.FieldValue(value = "defaultMQProducerImpl", declaringType = DefaultMQProducer.class)
            DefaultMQProducerImpl defaultMqProducerImpl) {
      // 从实际 producer 实例取出内部实现；Hook 实例由 RocketMqSingletons 统一提供。
      defaultMqProducerImpl.registerSendMessageHook(sendMessageHook());
    }
  }

  @SuppressWarnings("unused")
  public static class BatchSendAdvice {
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static Object[] onEnter(@Advice.This Object producer) {
      BatchSendState state = batchSendHelper().createBatchSendState(producer, false);
      // 下标 0 保存本次调用状态，下标 1 保存被覆盖的外层状态，退出时通过 @Advice.Enter 取回。
      // 此处只建立状态，真正的 Span 和消息属性要等 encode() 前才创建、注入。
      return new Object[] {state, currentBatchSendState().set(state)};
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class, inline = false)
    public static void onExit(
        @Advice.Enter Object[] enterResult, @Advice.Thrown Throwable throwable) {
      if (enterResult == null) {
        return;
      }
      BatchSendState state = (BatchSendState) enterResult[0];
      // 无论正常返回还是抛出异常，都先恢复外层调用，避免嵌套发送或线程复用时串用批次。
      currentBatchSendState().restore((BatchSendState) enterResult[1]);
      batchSendHelper().completeBatchSend(state, throwable);
    }
  }

  @SuppressWarnings("unused")
  public static class AsyncBatchSendArgumentOneAdvice {
    // value=1 是被插桩方法的第二个参数；index=2 是 onEnter 返回数组中的包装后 callback。
    @AssignReturned.ToArguments(@ToArgument(value = 1, index = 2))
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static Object[] onEnter(
        @Advice.This Object producer, @Advice.Argument(1) SendCallback callback) {
      // 记录是否有非空业务 callback；实际异步完成也可能由发送 Hook 观察到。
      BatchSendState state = batchSendHelper().createBatchSendState(producer, callback != null);
      Object[] enterResult = new Object[] {state, null, batchSendHelper().wrap(callback, state)};
      // 完成 callback 包装后再发布线程状态，供接下来同步执行的批量编码读取。
      enterResult[1] = currentBatchSendState().set(state);
      return enterResult;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class, inline = false)
    public static void onExit(
        @Advice.Enter Object[] enterResult, @Advice.Thrown Throwable throwable) {
      if (enterResult == null) {
        return;
      }
      BatchSendState state = (BatchSendState) enterResult[0];
      currentBatchSendState().restore((BatchSendState) enterResult[1]);
      batchSendHelper().completeBatchSend(state, throwable);
    }
  }

  @SuppressWarnings("unused")
  public static class AsyncBatchSendArgumentTwoAdvice {
    // 与上一个 Advice 的状态布局一致，仅把包装后的 callback 写回第三个参数。
    @AssignReturned.ToArguments(@ToArgument(value = 2, index = 2))
    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static Object[] onEnter(
        @Advice.This Object producer, @Advice.Argument(2) SendCallback callback) {
      BatchSendState state = batchSendHelper().createBatchSendState(producer, callback != null);
      Object[] enterResult = new Object[] {state, null, batchSendHelper().wrap(callback, state)};
      enterResult[1] = currentBatchSendState().set(state);
      return enterResult;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class, inline = false)
    public static void onExit(
        @Advice.Enter Object[] enterResult, @Advice.Thrown Throwable throwable) {
      if (enterResult == null) {
        return;
      }
      BatchSendState state = (BatchSendState) enterResult[0];
      currentBatchSendState().restore((BatchSendState) enterResult[1]);
      batchSendHelper().completeBatchSend(state, throwable);
    }
  }
}
