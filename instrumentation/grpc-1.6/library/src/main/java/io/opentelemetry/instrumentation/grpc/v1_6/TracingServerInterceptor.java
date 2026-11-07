/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * 服务端已注册方法的追踪入口：从请求 Metadata 提取远端上下文，创建 SERVER span，并包装调用及监听器。
 *
 * <p>请求可以包含多条流式消息，span 覆盖整个 RPC。close 先保存服务端选择的状态，正常结束由 onComplete 确认，取消由 onCancel
 * 处理；这里显式捕获的异常路径则直接结束 span 并继续抛出异常。
 */
final class TracingServerInterceptor implements ServerInterceptor {

  private static final AttributeKey<Boolean> GRPC_CANCELED =
      AttributeKey.booleanKey("grpc.canceled");
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
  private static final AtomicLongFieldUpdater<TracingServerCall> SENT_MESSAGE_ID_UPDATER =
      AtomicLongFieldUpdater.newUpdater(TracingServerCall.class, "sentMessageId");

  @SuppressWarnings("rawtypes") // AtomicLongFieldUpdater.newUpdater loses generic type
  private static final AtomicLongFieldUpdater<TracingServerCall> RECEIVED_MESSAGE_ID_UPDATER =
      AtomicLongFieldUpdater.newUpdater(TracingServerCall.class, "receivedMessageId");

  private final Instrumenter<GrpcRequest, Status> instrumenter;
  private final boolean captureExperimentalSpanAttributes;
  private final boolean emitMessageEvents;

  TracingServerInterceptor(
      Instrumenter<GrpcRequest, Status> instrumenter,
      boolean captureExperimentalSpanAttributes,
      boolean emitMessageEvents) {
    this.instrumenter = instrumenter;
    this.captureExperimentalSpanAttributes = captureExperimentalSpanAttributes;
    this.emitMessageEvents = emitMessageEvents;
  }

  @Override
  public <REQUEST, RESPONSE> ServerCall.Listener<REQUEST> interceptCall(
      ServerCall<REQUEST, RESPONSE> call,
      Metadata headers,
      ServerCallHandler<REQUEST, RESPONSE> next) {
    String authority = call.getAuthority();
    if (authority == null) {
      // 部分适配框架通过额外插桩保存 authority；读取同一个 ServerCall 上的补充值。
      // Armeria grpc server call does not implement getAuthority(). In
      // ArmeriaServerCallInstrumentation we store the value for the authority header in a virtual
      // field.
      authority = GrpcAuthorityStorage.getAuthority(call);
    }

    GrpcRequest request =
        GrpcRequest.createServerRequest(
            call.getMethodDescriptor(),
            headers,
            call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR),
            authority);
    Context parentContext = Context.current();
    if (!instrumenter.shouldStart(parentContext, request)) {
      return next.startCall(call, headers);
    }

    // 此 instrumenter 由 buildServerInstrumenter 创建，start 会以当前上下文为基础从 headers 提取远端父链路。
    Context context = instrumenter.start(parentContext, request);

    try (Scope ignored = context.makeCurrent()) {
      return new TracingServerCall<>(call, context, request).start(headers, next);
    } catch (Throwable e) {
      instrumenter.end(context, request, Status.UNKNOWN, e);
      throw e;
    }
  }

  final class TracingServerCall<REQUEST, RESPONSE>
      extends ForwardingServerCall.SimpleForwardingServerCall<REQUEST, RESPONSE> {
    private final Context context;
    private final GrpcRequest request;
    private Status status;

    // Used by SENT_MESSAGE_ID_UPDATER
    @SuppressWarnings("UnusedVariable")
    volatile long sentMessageId;

    // Used by RECEIVED_MESSAGE_ID_UPDATER
    @SuppressWarnings("UnusedVariable")
    volatile long receivedMessageId;

    TracingServerCall(
        ServerCall<REQUEST, RESPONSE> delegate, Context context, GrpcRequest request) {
      super(delegate);
      this.context = context;
      this.request = request;
    }

    TracingServerCallListener start(Metadata headers, ServerCallHandler<REQUEST, RESPONSE> next) {
      // 外层已激活 SERVER context。ContextStorageBridge 将它关联到当前 gRPC Context，
      // Contexts.interceptCall 再负责在后续业务 Listener 回调中 attach/detach 这个 gRPC Context。
      return new TracingServerCallListener(
          Contexts.interceptCall(io.grpc.Context.current(), this, headers, next), context, request);
    }

    @Override
    public void sendMessage(RESPONSE message) {
      // 服务端发送的是响应消息；大小记录和 SENT 事件都以服务端视角解释。
      request.setResponseSize(BodySizeUtil.getBodySize(message));
      try (Scope ignored = context.makeCurrent()) {
        super.sendMessage(message);
      }
      long messageId = SENT_MESSAGE_ID_UPDATER.incrementAndGet(this);
      if (emitMessageEvents) {
        Attributes attributes = Attributes.of(MESSAGE_TYPE, SENT, MESSAGE_ID, messageId);
        Span.fromContext(context).addEvent("message", attributes);
      }
    }

    @Override
    public void close(Status status, Metadata trailers) {
      // 发起关闭不等于监听器已收到完成通知，先保存状态供 onComplete 使用。
      this.status = status;
      try {
        delegate().close(status, trailers);
      } catch (Throwable e) {
        instrumenter.end(context, request, status, e);
        throw e;
      }
    }

    final class TracingServerCallListener
        extends ForwardingServerCallListener.SimpleForwardingServerCallListener<REQUEST> {
      private final Context context;
      private final GrpcRequest request;

      TracingServerCallListener(Listener<REQUEST> delegate, Context context, GrpcRequest request) {
        super(delegate);
        this.context = context;
        this.request = request;
      }

      private void end(Context context, GrpcRequest request, Status response, Throwable error) {
        // 结束时汇总该 RPC 的双向消息数；grpc.canceled 的判断使用外层 close 保存的 status。
        if (captureExperimentalSpanAttributes) {
          Span span = Span.fromContext(context);
          span.setAttribute(
              GRPC_RECEIVED_MESSAGE_COUNT, RECEIVED_MESSAGE_ID_UPDATER.get(TracingServerCall.this));
          span.setAttribute(
              GRPC_SENT_MESSAGE_COUNT, SENT_MESSAGE_ID_UPDATER.get(TracingServerCall.this));
          if (Status.CANCELLED.equals(status)) {
            span.setAttribute(GRPC_CANCELED, true);
          }
        }
        instrumenter.end(context, request, response, error);
      }

      @Override
      public void onMessage(REQUEST message) {
        // 这里收到的是请求消息；真正的业务 delegate 由 Contexts.interceptCall 恢复 gRPC/OTel 上下文。
        request.setRequestSize(BodySizeUtil.getBodySize(message));
        long messageId = RECEIVED_MESSAGE_ID_UPDATER.incrementAndGet(TracingServerCall.this);
        if (emitMessageEvents) {
          Attributes attributes = Attributes.of(MESSAGE_TYPE, RECEIVED, MESSAGE_ID, messageId);
          Span.fromContext(context).addEvent("message", attributes);
        }
        delegate().onMessage(message);
      }

      @Override
      public void onHalfClose() {
        // 客户端已发完请求，但服务端仍可继续处理和发送响应，因此正常返回时不结束 span。
        try {
          delegate().onHalfClose();
        } catch (Throwable e) {
          instrumenter.end(context, request, Status.UNKNOWN, e);
          throw e;
        }
      }

      @Override
      public void onCancel() {
        // 先让业务处理取消；若回调自身抛错，记录 UNKNOWN 和异常，否则以 CANCELLED 结束。
        try {
          delegate().onCancel();
        } catch (Throwable e) {
          end(context, request, Status.UNKNOWN, e);
          throw e;
        }
        end(context, request, Status.CANCELLED, null);
      }

      @Override
      public void onComplete() {
        // 此时 RPC 生命周期已完成。业务回调正常返回后，使用 close 捕获的状态结算遥测。
        try {
          delegate().onComplete();
        } catch (Throwable e) {
          end(context, request, Status.UNKNOWN, e);
          throw e;
        }
        if (status == null) {
          // 没观察到 close 的状态时保留“未知”，不把缺失信息当成成功。
          status = Status.UNKNOWN;
        }
        end(context, request, status, status.getCause());
      }

      @Override
      public void onReady() {
        // 就绪通知不会正常结束 RPC；只有该回调向外抛异常时才走这里的异常结束路径。
        try {
          delegate().onReady();
        } catch (Throwable e) {
          end(context, request, Status.UNKNOWN, e);
          throw e;
        }
      }
    }
  }
}
