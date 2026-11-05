/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0.RocketMqSingletons.simpleConsumerReceiveInstrumenter;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil;
import io.opentelemetry.instrumentation.api.internal.Timer;
import io.opentelemetry.javaagent.bootstrap.ExceptionLogger;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.FutureCallback;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.Futures;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.ListenableFuture;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.MoreExecutors;

/**
 * 一次 SimpleConsumer 拉取的状态，跨异步边界保存消费组、入口 Context 和计时器。
 *
 * <p>结果到达后才创建并结束遥测，因此可以跳过空结果，并利用实际消息填写 Topic 和批次属性。 这里只保存 Context，不在拉取期间保持线程
 * Scope，避免把异步操作的生命周期绑定到调用线程。
 */
public class SimpleConsumerReceiveOperation {

  private final String consumerGroup;
  private final Context parentContext;
  private final Timer timer;

  /**
   * Returns whether the receive that {@code consumer} is about to perform is recorded here instead
   * of by the per-message-queue instrumentation underneath it.
   */
  public static boolean handlesReceive(Object consumer) {
    return consumer instanceof SimpleConsumer;
  }

  public static SimpleConsumerReceiveOperation start(SimpleConsumer consumer) {
    // 在应用进入 receive0 时捕获，完成线程的环境上下文不参与父上下文选择。
    return new SimpleConsumerReceiveOperation(
        consumer.getConsumerGroup(), Context.current(), Timer.start());
  }

  private SimpleConsumerReceiveOperation(String consumerGroup, Context parentContext, Timer timer) {
    this.consumerGroup = consumerGroup;
    this.parentContext = parentContext;
    this.timer = timer;
  }

  public ListenableFuture<List<MessageView>> wrap(
      ListenableFuture<List<MessageView>> originalFuture) {
    // transform 的转换函数只处理成功结果，所以原 Future 另挂失败回调；成功分支在此留空。
    Futures.addCallback(
        originalFuture,
        new FutureCallback<List<MessageView>>() {
          @Override
          public void onSuccess(List<MessageView> unused) {}

          @Override
          public void onFailure(Throwable t) {
            endSafely(null, t);
          }
        },
        MoreExecutors.directExecutor());
    // 返回转换后的 Future，使成功接收的遥测先完成，再把原消息列表交给上层调用者。
    // directExecutor 不额外排队，可能在完成线程或注册时的当前线程执行。
    return Futures.transform(
        originalFuture,
        messages -> {
          endSuccessfully(messages);
          return messages;
        },
        MoreExecutors.directExecutor());
  }

  private void endSuccessfully(List<MessageView> messages) {
    // Future 中的回调不在 Advice 的异常抑制范围内，必须在这里隔离并记录插桩自身的异常。
    try {
      end(messages, null);
    } catch (Throwable t) {
      ExceptionLogger.logSuppressedError("Error instrumenting RocketMQ SimpleConsumer receive", t);
    }
  }

  private void endSafely(@Nullable List<MessageView> messages, Throwable error) {
    // 遥测失败不能覆盖原始拉取异常，也不能让失败回调再向 SDK 抛出新的插桩异常。
    try {
      end(messages, error);
    } catch (Throwable t) {
      ExceptionLogger.logSuppressedError(
          "Error instrumenting RocketMQ SimpleConsumer receive failure", t);
    }
  }

  private void end(@Nullable List<MessageView> messages, @Nullable Throwable error) {
    // 空列表表示成功但没有消息；失败路径则保留错误遥测，即使此时完全拿不到消息信息。
    if (error == null && messages != null && messages.isEmpty()) {
      return;
    }
    // 成功路径已排除空列表，可从消息恢复 Topic/namespace；失败时只保留入口获取的消费组。
    RocketMqReceiveRequest request =
        messages == null
            ? RocketMqReceiveRequest.create(consumerGroup)
            : RocketMqReceiveRequest.create(consumerGroup, messages);
    Instrumenter<RocketMqReceiveRequest, List<MessageView>> instrumenter =
        simpleConsumerReceiveInstrumenter();
    if (instrumenter.shouldStart(parentContext, request)) {
      // 使用入口捕获的父上下文和起始时间，Span 耗时涵盖 receive0 内部的异步工作。
      InstrumenterUtil.startAndEnd(
          instrumenter, parentContext, request, messages, error, timer.startTime(), timer.now());
    }
  }
}
