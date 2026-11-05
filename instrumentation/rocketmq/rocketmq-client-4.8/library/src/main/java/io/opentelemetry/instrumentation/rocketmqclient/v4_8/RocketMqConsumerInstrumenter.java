/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.common.message.MessageExt;

// 按实际交给 Listener 的消息数选择处理模型；消息由一次批量发送产生，也可能被分次消费。
// 单条可采用消息上下文作为父；多条只建立一个 Process Span，并逐条添加来源 Link。
final class RocketMqConsumerInstrumenter {

  private final Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext>
      singleProcessInstrumenter;
  private final Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext>
      batchProcessInstrumenter;

  RocketMqConsumerInstrumenter(
      Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext> singleProcessInstrumenter,
      Instrumenter<RocketMqConsumerRequest, ConsumeMessageContext> batchProcessInstrumenter) {
    this.singleProcessInstrumenter = singleProcessInstrumenter;
    this.batchProcessInstrumenter = batchProcessInstrumenter;
  }

  @Nullable
  ConsumerContext start(
      Context parentContext,
      List<MessageExt> msgs,
      String consumerGroup,
      @Nullable String namespace) {
    int batchSize = msgs.size();
    // 空列表已由调用方 Hook 拦截；集合中只有一条消息时仍使用单条消费的父上下文规则。
    if (batchSize == 1) {
      RocketMqConsumerRequest request =
          new RocketMqConsumerRequest(msgs.get(0), consumerGroup, batchSize, namespace);
      if (singleProcessInstrumenter.shouldStart(parentContext, request)) {
        Context context = singleProcessInstrumenter.start(parentContext, request);
        return new ConsumerContext(context, request);
      }
      return null;
    }

    // 一个请求代表整个消费批次，避免按消息数创建多个覆盖同一次 Listener 调用的 Process Span。
    RocketMqConsumerRequest request =
        new RocketMqConsumerRequest(msgs, consumerGroup, batchSize, namespace);
    if (!batchProcessInstrumenter.shouldStart(parentContext, request)) {
      return null;
    }
    Context context = batchProcessInstrumenter.start(parentContext, request);
    return new ConsumerContext(context, request);
  }

  void end(ConsumerContext consumerContext, ConsumeMessageContext response) {
    // 使用开始时保存的请求决定结束哪个 Instrumenter，避免从完成时线程上下文重新推导。
    // SDK Hook 没有提供原始业务 Throwable，错误分类依赖 response 中的消费返回类型与状态。
    RocketMqConsumerRequest request = consumerContext.getRequest();
    if (request.getBatchSize() == 1) {
      singleProcessInstrumenter.end(consumerContext.getContext(), request, response, null);
      return;
    }
    batchProcessInstrumenter.end(consumerContext.getContext(), request, response, null);
  }

  static final class ConsumerContext {
    // 绑定同一次 start/end 的 Context 与请求；Scope 由外层 Hook 的 ContextAndScope 单独持有。
    private final Context context;
    private final RocketMqConsumerRequest request;

    private ConsumerContext(Context context, RocketMqConsumerRequest request) {
      this.context = context;
      this.request = request;
    }

    Context getContext() {
      return context;
    }

    RocketMqConsumerRequest getRequest() {
      return request;
    }
  }
}
