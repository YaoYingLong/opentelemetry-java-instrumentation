/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0.RocketMqSingletons.consumerProcessInstrumenter;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * 以一次 MessageListener.consume 调用作为 Process Span 的边界。
 *
 * <p>同一消息再次投递时会再次进入 consume，分别记录每次处理尝试；Span 在监听器返回或抛异常时结束， 不等待 SDK 随后的 ACK、重投递或其他协议处理。
 */
public class MessageListenerWrapper implements MessageListener {
  private final MessageListener delegator;

  private MessageListenerWrapper(MessageListener delegator) {
    this.delegator = delegator;
  }

  public static MessageListener wrapIfNeeded(MessageListener messageListener) {
    if (messageListener instanceof MessageListenerWrapper) {
      return messageListener;
    }
    return new MessageListenerWrapper(messageListener);
  }

  @Override
  public ConsumeResult consume(MessageView messageView) {
    // 接收与处理通常不在同一线程：优先恢复接收完成时附着到消息上的环境上下文。
    // 公共 Process Instrumenter 再结合消息携带的生产端上下文，确定父 Span 和 Link。
    Context parentContext = VirtualFieldStore.getContextByMessage(messageView);
    if (parentContext == null) {
      parentContext = Context.current();
    }
    Instrumenter<MessageView, ConsumeResult> processInstrumenter = consumerProcessInstrumenter();
    if (!processInstrumenter.shouldStart(parentContext, messageView)) {
      return delegator.consume(messageView);
    }
    Context context = processInstrumenter.start(parentContext, messageView);

    ConsumeResult consumeResult;
    try (Scope ignored = context.makeCurrent()) {
      // 业务监听器内部的下游调用以 Process 为当前上下文，离开回调后自动恢复消费线程原有状态。
      consumeResult = delegator.consume(messageView);
    } catch (Throwable t) {
      // 记录业务异常后原样抛回 SDK，保留 SDK 自己的失败与重试处理语义。
      processInstrumenter.end(context, messageView, null, t);
      throw t;
    }
    // 即便没有抛异常，FAILURE 返回值也会由工厂配置的状态提取器标记为错误。
    processInstrumenter.end(context, messageView, consumeResult, null);
    return consumeResult;
  }
}
