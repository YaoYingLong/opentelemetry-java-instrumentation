/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.GrpcTargetParser;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.ParsedTarget;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nullable;

/**
 * 客户端 RPC 的追踪入口：一次调用对应一个 CLIENT span，流式调用中的多条消息记录为该 span 的事件。
 *
 * <p>gRPC 将“创建调用”和“发送请求头”分成两个阶段：interceptCall 创建 span，ClientCall.start 注入传播头， Listener.onClose
 * 根据最终状态结束 span。每次 makeCurrent 只覆盖当前线程的一段执行，Scope 关闭不代表 RPC 已结束。
 */
final class TracingClientInterceptor implements ClientInterceptor {

  private static final AttributeKey<Long> GRPC_RECEIVED_MESSAGE_COUNT =
      AttributeKey.longKey("grpc.received.message_count");
  private static final AttributeKey<Long> GRPC_SENT_MESSAGE_COUNT =
      AttributeKey.longKey("grpc.sent.message_count");
  // copied from MessageIncubatingAttributes
  private static final AttributeKey<Long> MESSAGE_ID = AttributeKey.longKey("message.id");
  private static final AttributeKey<String> MESSAGE_TYPE = AttributeKey.stringKey("message.type");
  // copied from MessageIncubatingAttributes.MessageTypeValues
  private static final String SENT = "SENT";
  private static final String RECEIVED = "RECEIVED";

  @SuppressWarnings("rawtypes") // AtomicLongFieldUpdater.newUpdater loses generic type
  private static final AtomicLongFieldUpdater<TracingClientCall> SENT_MESSAGE_ID_UPDATER =
      AtomicLongFieldUpdater.newUpdater(TracingClientCall.class, "sentMessageId");

  @SuppressWarnings("rawtypes") // AtomicLongFieldUpdater.newUpdater loses generic type
  private static final AtomicLongFieldUpdater<TracingClientCall> RECEIVED_MESSAGE_ID_UPDATER =
      AtomicLongFieldUpdater.newUpdater(TracingClientCall.class, "receivedMessageId");

  private final Instrumenter<GrpcRequest, Status> instrumenter;
  private final ContextPropagators propagators;
  private final boolean captureExperimentalSpanAttributes;
  private final boolean emitMessageEvents;
  @Nullable private final ParsedTarget parsedTarget;

  TracingClientInterceptor(
      Instrumenter<GrpcRequest, Status> instrumenter,
      ContextPropagators propagators,
      boolean captureExperimentalSpanAttributes,
      boolean emitMessageEvents,
      @Nullable String target,
      boolean computedTarget) {
    this.instrumenter = instrumenter;
    this.propagators = propagators;
    this.captureExperimentalSpanAttributes = captureExperimentalSpanAttributes;
    this.emitMessageEvents = emitMessageEvents;
    // 一个拦截器可服务同一 channel 上的多次调用，因此 target 在构造时解析一次。
    // computedTarget 表示 gRPC 已完成 resolver 选择，与用户直接传入的原始 target 采用不同的歧义处理。
    this.parsedTarget =
        computedTarget
            ? GrpcTargetParser.parseComputedTarget(target)
            : GrpcTargetParser.parse(target);
  }

  @Override
  public <REQUEST, RESPONSE> ClientCall<REQUEST, RESPONSE> interceptCall(
      MethodDescriptor<REQUEST, RESPONSE> method, CallOptions callOptions, Channel next) {
    GrpcRequest request = GrpcRequest.createClientRequest(method, next.authority(), parsedTarget);
    // 同时保留调用方上下文和稍后创建的 RPC 上下文：前者用于业务回调，后者用于 gRPC 内部执行及跨进程传播。
    Context parentContext = Context.current();
    if (!instrumenter.shouldStart(parentContext, request)) {
      return next.newCall(method, callOptions);
    }

    Context context = instrumenter.start(parentContext, request);
    ClientCall<REQUEST, RESPONSE> result;
    try (Scope ignored = context.makeCurrent()) {
      // 在 CLIENT span 的作用域中创建下游调用；这里返回的仍是 ClientCall，并不表示收到响应。
      result = next.newCall(method, callOptions);
    } catch (Throwable t) {
      instrumenter.end(context, request, Status.UNKNOWN, t);
      throw t;
    }

    return new TracingClientCall<>(result, parentContext, context, request);
  }

  // 每个 RPC 独立持有上下文、属性和消息计数，后续异步回调通过该包装器取回它们。
  final class TracingClientCall<REQUEST, RESPONSE>
      extends ForwardingClientCall.SimpleForwardingClientCall<REQUEST, RESPONSE> {

    private final Context parentContext;
    private final Context context;
    private final GrpcRequest request;

    // Used by SENT_MESSAGE_ID_UPDATER
    @SuppressWarnings("UnusedVariable")
    volatile long sentMessageId;

    // Used by RECEIVED_MESSAGE_ID_UPDATER
    @SuppressWarnings("UnusedVariable")
    volatile long receivedMessageId;

    TracingClientCall(
        ClientCall<REQUEST, RESPONSE> delegate,
        Context parentContext,
        Context context,
        GrpcRequest request) {
      super(delegate);
      this.parentContext = parentContext;
      this.context = context;
      this.request = request;
    }

    @Override
    public void start(Listener<RESPONSE> responseListener, Metadata headers) {
      // 此时才拿到将要发送的 Metadata，注入的是包含 CLIENT span 的 context，而不是 parentContext。
      propagators.getTextMapPropagator().inject(context, headers, MetadataSetter.INSTANCE);
      // 保存请求头供结束阶段的属性提取器读取；在 interceptCall 启动 span 时，请求头尚不可用。
      request.setMetadata(headers);
      try (Scope ignored = context.makeCurrent()) {
        super.start(
            new TracingClientCallListener(responseListener, parentContext, context, request),
            headers);
      } catch (Throwable e) {
        instrumenter.end(context, request, Status.UNKNOWN, e);
        throw e;
      }
    }

    @Override
    public void sendMessage(REQUEST message) {
      // 只读取可识别消息的序列化大小；这里覆盖最近一条消息的大小，并非累计整个流的字节数。
      request.setRequestSize(BodySizeUtil.getBodySize(message));
      try (Scope ignored = context.makeCurrent()) {
        super.sendMessage(message);
      } catch (Throwable e) {
        instrumenter.end(context, request, Status.UNKNOWN, e);
        throw e;
      }
      // 发送和接收分别从 1 编号；即使关闭逐消息事件，计数仍更新，供实验性总消息数属性使用。
      long messageId = SENT_MESSAGE_ID_UPDATER.incrementAndGet(this);
      if (emitMessageEvents) {
        Attributes attributes = Attributes.of(MESSAGE_TYPE, SENT, MESSAGE_ID, messageId);
        Span.fromContext(context).addEvent("message", attributes);
      }
    }

    final class TracingClientCallListener
        extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RESPONSE> {

      private final Context parentContext;
      private final Context context;
      private final GrpcRequest request;

      TracingClientCallListener(
          Listener<RESPONSE> delegate,
          Context parentContext,
          Context context,
          GrpcRequest request) {
        super(delegate);
        this.parentContext = parentContext;
        this.context = context;
        this.request = request;
      }

      @Override
      public void onMessage(RESPONSE message) {
        // 一条响应消息不等于 RPC 结束，服务端流式/双向流式调用还可能继续返回消息。
        request.setResponseSize(BodySizeUtil.getBodySize(message));
        long messageId = RECEIVED_MESSAGE_ID_UPDATER.incrementAndGet(TracingClientCall.this);
        if (emitMessageEvents) {
          Attributes attributes = Attributes.of(MESSAGE_TYPE, RECEIVED, MESSAGE_ID, messageId);
          Span.fromContext(context).addEvent("message", attributes);
        }
        // 业务处理响应时恢复发起调用前的上下文，使回调里新发起的操作使用调用方的父链路。
        try (Scope ignored = parentContext.makeCurrent()) {
          delegate().onMessage(message);
        }
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        // 连接建立前可能没有远端地址，因此到终止回调再读取实际 socket 对端；它与配置目标可能不同。
        request.setPeerSocketAddress(getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
        if (captureExperimentalSpanAttributes) {
          Span span = Span.fromContext(context);
          span.setAttribute(
              GRPC_RECEIVED_MESSAGE_COUNT, RECEIVED_MESSAGE_ID_UPDATER.get(TracingClientCall.this));
          span.setAttribute(
              GRPC_SENT_MESSAGE_COUNT, SENT_MESSAGE_ID_UPDATER.get(TracingClientCall.this));
        }
        // 最终 gRPC Status 决定 RPC 结果；先结束 CLIENT span，再在调用方上下文中通知业务完成。
        instrumenter.end(context, request, status, status.getCause());
        try (Scope ignored = parentContext.makeCurrent()) {
          delegate().onClose(status, trailers);
        }
      }

      @Override
      public void onReady() {
        // onReady 只是流量控制中的可发送通知，也属于业务回调，不是 RPC 的完成信号。
        try (Scope ignored = parentContext.makeCurrent()) {
          delegate().onReady();
        }
      }
    }
  }
}
