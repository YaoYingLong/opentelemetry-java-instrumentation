/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.rocketmqclient.v4_8;

import static io.opentelemetry.javaagent.instrumentation.rocketmqclient.v4_8.RocketMqSingletons.currentBatchSendState;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

// javaagent 批量发送的生命周期协调器：集合发送入口捕获调用方，编码前建立消息上下文，
// 底层 Hook 补充发送结果，方法退出或异步完成结束 Send Span。
// 本类属于 javaagent 模块，但与 library 的内部类同包，以复用包可见的工厂、Getter 和提取器。
public final class RocketMqBatchSendHelper {

  // 将状态绑定在外层 MessageBatch 上，使异步线程上的发送 Hook 也能找到发起时的上下文。
  private static final VirtualField<Message, BatchSendState> BATCH_SEND_STATE =
      VirtualField.find(Message.class, BatchSendState.class);

  private final Instrumenter<SendMessageContext, Void> singleSendInstrumenter;
  private final Instrumenter<SendMessageContext, Void> sendInstrumenter;
  private final Instrumenter<SendMessageContext, Void> createInstrumenter;
  private final TextMapPropagator propagator;
  private final MessageExtractAdapter getter = new MessageExtractAdapter();

  public RocketMqBatchSendHelper(
      OpenTelemetry openTelemetry,
      // 把选中的消息属性记录到Span上，便于按业务属性查询和排查消息链路
      IncludeExclude headers,
      boolean captureExperimentalSpanAttributes,
      boolean messageCreationSpansEnabled) {
    singleSendInstrumenter =
        RocketMqInstrumenterFactory.createProducerInstrumenter(
            openTelemetry, headers, captureExperimentalSpanAttributes);
    sendInstrumenter =
        RocketMqInstrumenterFactory.createBatchProducerInstrumenter(
            openTelemetry, headers, captureExperimentalSpanAttributes);
    // 此开关仅决定是否创建逐条 Create Span；关闭后仍支持批量发送、Send Span 和上下文传播。
    createInstrumenter =
        RocketMqInstrumenterFactory.createMessageCreateInstrumenter(
            openTelemetry, headers, messageCreationSpansEnabled);
    propagator = openTelemetry.getPropagators().getTextMapPropagator();
  }

  public BatchSendState createBatchSendState(Object producer, boolean callbackCompletionExpected) {
    // 必须在调用方线程捕获完整 Context；完成线程的 Context 可能属于其他操作。
    return new BatchSendState(
        Context.current(),
        RocketMqNamespaceUtil.getNamespace(producer),
        callbackCompletionExpected);
  }

  public void beforeBatchEncode(Message batch) {
    BatchSendState state = currentBatchSendState().get();
    // 没有集合发送入口状态，或本次状态已准备过时跳过，避免独立编码或重复编码再创建 Span。
    if (state == null || state.request != null) {
      return;
    }
    state.prepare(batch);
  }

  public void completeBatchSend(BatchSendState state, @Nullable Throwable error) {
    // 方法抛错时立即收尾。正常返回只有在既不等待 callback、也未被 Hook 标记为 ASYNC 时结束；
    // 异步提交成功不代表发送完成，后续由 Hook 或包装后的 callback 结束 Span。
    if (error != null || (!state.callbackCompletionExpected && !state.wasClaimedAsynchronously())) {
      state.end(error);
    }
  }

  @Nullable
  public SendCallback wrap(@Nullable SendCallback delegate, BatchSendState state) {
    // 保留 null callback 的传参语义，该路径是否异步由实际发送 Hook 的 communicationMode 补充。
    if (delegate == null) {
      return delegate;
    }
    return new SendCallback() {
      @Override
      public void onSuccess(SendResult sendResult) {
        state.end(null);
        // 业务 callback 使用调用方 Context；其中新建的 Span 与 Send 同属调用方，而非 Send 的子 Span。
        // try-with-resources 会在 callback 返回或抛错后恢复完成线程原有的 Context。
        try (Scope ignored = state.parentContext.makeCurrent()) {
          delegate.onSuccess(sendResult);
        }
      }

      @Override
      public void onException(Throwable e) {
        // 包装 callback 也覆盖排队超时等尚未进入发送 Hook 的失败。
        state.end(e);
        try (Scope ignored = state.parentContext.makeCurrent()) {
          delegate.onException(e);
        }
      }
    };
  }

  public SendMessageHook createSendMessageHook() {
    SendMessageHook delegate = new TracingSendMessageHookImpl(singleSendInstrumenter);
    return new SendMessageHook() {
      @Override
      public String hookName() {
        return delegate.hookName();
      }

      @Override
      public void sendMessageBefore(SendMessageContext context) {
        BatchSendState state = getState(context);
        if (state == null) {
          delegate.sendMessageBefore(context);
          return;
        }
        // 编码前已经创建 Send Span，此处只补齐 SDK 才知道的 Broker 地址、模式等信息。
        state.copyFrom(context);
      }

      @Override
      public void sendMessageAfter(SendMessageContext context) {
        BatchSendState state = getState(context);
        if (state == null) {
          delegate.sendMessageAfter(context);
          return;
        }
        state.copyFrom(context);
        // ASYNC 发起阶段也可能调用 After，但结果和异常都为空；必须等到实际完成才结束。
        // 同步发送由集合方法的退出 Advice 收尾，使一个 Send Span 覆盖整个批量调用。
        if ((context.getSendResult() != null || context.getException() != null)
            && CommunicationMode.ASYNC == context.getCommunicationMode()) {
          state.end(context.getException());
        }
      }
    };
  }

  @Nullable
  private static BatchSendState getState(@Nullable SendMessageContext context) {
    return context == null || context.getMessage() == null
        ? null
        : BATCH_SEND_STATE.get(context.getMessage());
  }

  public final class BatchSendState {
    // parentContext 是用户调用 send 时的上下文，sendContext 是本次批量发送的上下文，两者职责不同。
    private final Context parentContext;
    @Nullable private final String namespace;
    private final boolean callbackCompletionExpected;

    @Nullable private BatchSendContext request;
    @Nullable private Context sendContext;
    private boolean ended;

    private BatchSendState(
        Context parentContext, @Nullable String namespace, boolean callbackCompletionExpected) {
      this.parentContext = parentContext;
      this.namespace = namespace;
      this.callbackCompletionExpected = callbackCompletionExpected;
    }

    private void prepare(Message batch) {
      // 此时 SDK 已为各条消息校验并设置唯一 ID，但还没有把 properties 编码进批次 body。
      request = new BatchSendContext(batch, namespace);
      List<Context> creationContexts = new ArrayList<>();
      List<Message> messagesWithoutCreationContext = new ArrayList<>();
      List<Context> extractedContextsWithoutCreationContext = new ArrayList<>();
      // 去掉父 Span，避免消息没携带 trace 信息时误把调用方 Span 当成已有创建上下文；
      // 其他 Context 值仍保留，后续也保留 propagator 提取出的 baggage 等内容。
      Context extractionContext = parentContext.with(Span.getInvalid());
      for (Object item : (Iterable<?>) batch) {
        Message message = (Message) item;
        // 把message中的上下文提取出来合并extractionContext中的内容生成creationContext
        // 如果message的property中有traceparent会被截写出SpanContext并添加到creationContext中
        Context creationContext = propagator.extract(extractionContext, message, getter);
        // 有效的自定义上下文直接沿用；只有缺少上下文时才尝试创建新的 Create Span。
        if (!Span.fromContext(creationContext).getSpanContext().isValid()) {
          MessageCreateContext createRequest = new MessageCreateContext(message, namespace);
          // 如果默认是关闭创建逐条 Create Span时shouldStart返回false
          if (createInstrumenter.shouldStart(parentContext, createRequest)) {
            // 生成create topic的span数据，也会看是否通过配置开启该导出
            Context createdContext = createInstrumenter.start(parentContext, createRequest);
            // Create 是建立逐条消息身份的短操作，立即结束；其 SpanContext 在结束后仍然有效。
            // 每个 Create 都以捕获的调用方为父，后面创建的 Send 也是同一个调用方的子 Span。
            createInstrumenter.end(createdContext, createRequest, null, null);
            creationContext = creationContext.with(Span.fromContext(createdContext));
            // 将传播的属性注入到每条消息中
            propagator.inject(creationContext, message, MessagePropertySetter.INSTANCE);
          }
        }
        // 如果默认是关闭创建逐条 Create Span时shouldStart返回false
        if (!Span.fromContext(creationContext).getSpanContext().isValid()) {
          // 关闭 Create 或受到抑制时，保留这些消息，等 Send 创建之后再统一补齐传播上下文。
          messagesWithoutCreationContext.add(message);
          extractedContextsWithoutCreationContext.add(creationContext);
        }
        creationContexts.add(creationContext);
      }

      // 必须在 start() 前登记：创建 Send 时既要生成 Links，也要据此决定 SpanKind。
      // 全部有效时 Send 为 CLIENT；任意消息仍缺少上下文时，Send 将承担传播职责并使用 PRODUCER。
      RocketMqBatchSendSpanLinksExtractor.setContexts(request, creationContexts);
      if (sendInstrumenter.shouldStart(parentContext, request)) {
        sendContext = sendInstrumenter.start(parentContext, request);
      }
      // Send 被抑制时仍可传播调用方上下文；已有创建上下文的消息不进入这段兜底注入。
      Context fallbackContext = sendContext == null ? parentContext : sendContext;
      for (int i = 0; i < messagesWithoutCreationContext.size(); i++) {
        // 向massage中对应的SpanContext为无效的注入传播信息和SpanContext
        propagator.inject(
            // 原始的parentContext去掉SpanContext后在从message中提取，然后在通过当前的sendInstrumenter生成的SpanContext补回Context
            extractedContextsWithoutCreationContext.get(i).with(Span.fromContext(fallbackContext)),
            messagesWithoutCreationContext.get(i),
            MessagePropertySetter.INSTANCE);
      }
      // 线程状态只服务于编码阶段；后续 Hook 根据实际 MessageBatch 查找状态，不依赖发起线程。
      BATCH_SEND_STATE.set(batch, this);
    }

    private void copyFrom(SendMessageContext context) {
      if (request == null) {
        return;
      }
      // request 是编码前构造的遥测请求，context 是 SDK 后来构造的发送请求；把结果回填到前者。
      request.setBrokerAddr(context.getBrokerAddr());
      request.setCommunicationMode(context.getCommunicationMode());
      request.setException(context.getException());
      request.setSendResult(context.getSendResult());
    }

    private boolean wasClaimedAsynchronously() {
      return request != null && CommunicationMode.ASYNC == request.getCommunicationMode();
    }

    private void end(@Nullable Throwable error) {
      // Hook、callback 和方法退出都可能尝试收尾，此标记避免已结束的状态再次处理。
      if (ended) {
        return;
      }
      ended = true;
      if (sendContext != null && request != null) {
        sendInstrumenter.end(sendContext, request, null, error);
      }
      if (request != null) {
        // 属性与指标在 instrumenter.end() 中仍需读取消息数量等状态，因此完成后才解除关联。
        RocketMqBatchSendSpanLinksExtractor.clearContexts(request);
        BATCH_SEND_STATE.set(request.getMessage(), null);
      }
    }
  }

  private static class MessageCreateContext extends SendMessageContext {
    // 编码前还没有 SDK 的 SendMessageContext，使用这个请求保存消息及 producer 的 namespace。
    @Nullable private final String namespace;

    private MessageCreateContext(Message message, @Nullable String namespace) {
      setMessage(message);
      this.namespace = namespace;
    }

    @Nullable
    @Override
    public String getNamespace() {
      return namespace;
    }
  }

  private static final class BatchSendContext extends MessageCreateContext {
    private BatchSendContext(Message message, @Nullable String namespace) {
      super(message, namespace);
    }
  }

  private enum MessagePropertySetter implements TextMapSetter<Message> {
    INSTANCE;

    @Override
    public void set(@Nullable Message carrier, String key, String value) {
      if (carrier != null) {
        // 写入的是实际内层消息，接下来的 MessageBatch.encode() 会把传播字段一起序列化。
        carrier.getProperties().put(key, value);
      }
    }
  }
}
