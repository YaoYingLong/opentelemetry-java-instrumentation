/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0.RocketMqSingletons.consumerReceiveInstrumenter;
import static java.util.Collections.emptyList;

import apache.rocketmq.v2.ReceiveMessageRequest;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil;
import io.opentelemetry.instrumentation.api.internal.Timer;
import java.util.List;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.impl.consumer.ReceiveMessageResult;
import org.apache.rocketmq.shaded.com.google.common.util.concurrent.FutureCallback;

/**
 * PushConsumer 底层接收的完成回调：准备后续 Process 所需的消息状态，并按原始计时补记 Receive。
 *
 * <p>成功但没有消息时不记录遥测；失败时即使没有消息，也使用协议请求中的 Topic 和消费组记录错误。
 */
public class ReceiveSpanFinishingCallback implements FutureCallback<ReceiveMessageResult> {

  private final ReceiveMessageRequest request;
  private final Timer timer;

  public ReceiveSpanFinishingCallback(ReceiveMessageRequest request, Timer timer) {
    this.request = request;
    this.timer = timer;
  }

  @Override
  public void onSuccess(ReceiveMessageResult receiveMessageResult) {
    List<MessageView> messageViews = receiveMessageResult.getMessageViews();
    // 空轮询不产生 Receive Span 或对应指标，避免把没有消息的等待计为一次消息接收。
    if (messageViews.isEmpty()) {
      return;
    }
    String consumerGroup = request.getGroup().getName();
    // MessageView 本身没有消费组访问器，先从请求取出并附着到每条消息，供 Process 属性提取器使用。
    for (MessageView messageView : messageViews) {
      VirtualFieldStore.setConsumerGroupByMessage(messageView, consumerGroup);
    }
    Instrumenter<RocketMqReceiveRequest, List<MessageView>> receiveInstrumenter =
        consumerReceiveInstrumenter();
    Context parentContext = Context.current();
    // 这里读取的是完成回调执行时的环境上下文，并未在接收入口显式捕获 Context。
    // 把这个环境上下文交给 Process，而不是把稍后补记的 Receive Span 当作 Process 的父 Span。
    for (MessageView messageView : messageViews) {
      VirtualFieldStore.setContextByMessage(messageView, parentContext);
    }
    RocketMqReceiveRequest receiveRequest = RocketMqReceiveRequest.create(request, messageViews);
    if (receiveInstrumenter.shouldStart(parentContext, receiveRequest)) {
      // 已知结果后一次性 start/end，但显式传入接收入口时间，耗时仍覆盖整个异步接收阶段。
      InstrumenterUtil.startAndEnd(
          receiveInstrumenter,
          parentContext,
          receiveRequest,
          messageViews,
          null,
          timer.startTime(),
          timer.now());
    }
  }

  @Override
  public void onFailure(Throwable throwable) {
    // 请求仍提供 Topic、namespace 和消费组；响应传 null 表示失败，不能当成“成功收到 0 条”。
    Instrumenter<RocketMqReceiveRequest, List<MessageView>> receiveInstrumenter =
        consumerReceiveInstrumenter();
    Context parentContext = Context.current();
    RocketMqReceiveRequest receiveRequest = RocketMqReceiveRequest.create(request, emptyList());
    if (receiveInstrumenter.shouldStart(parentContext, receiveRequest)) {
      InstrumenterUtil.startAndEnd(
          receiveInstrumenter,
          parentContext,
          receiveRequest,
          null,
          throwable,
          timer.startTime(),
          timer.now());
    }
  }
}
