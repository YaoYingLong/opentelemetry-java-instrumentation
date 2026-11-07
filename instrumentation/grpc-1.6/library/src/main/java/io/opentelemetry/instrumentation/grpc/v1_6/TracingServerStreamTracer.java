/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.internal.InstrumenterUtil;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ServerStreamTracer} that creates spans for requests to unregistered gRPC methods, which
 * server interceptors do not see.
 *
 * <p>服务端找不到方法时不会进入常规拦截器，因此从传输流关闭事件补建 span；已创建 ServerCall 的请求交给常规路径。
 */
final class TracingServerStreamTracer extends ServerStreamTracer {

  private static final String UNKNOWN_METHOD_NAME = "_OTHER";
  private static final io.grpc.Context.Key<AtomicBoolean> STREAM_TRACER_STATE_KEY =
      io.grpc.Context.key("otel-grpc-stream-tracer-state");

  private final Instrumenter<GrpcRequest, Status> instrumenter;
  private final ContextPropagators propagators;
  private final String fullMethodName;
  private final Metadata headers;
  private final Context parentContext;
  // span 到关闭时才补建，但开始时间取自流创建阶段，避免把整个失败请求记录为接近零耗时。
  private final Instant startTime;

  private volatile AtomicBoolean handledOrSpanStarted = new AtomicBoolean();

  TracingServerStreamTracer(
      Instrumenter<GrpcRequest, Status> instrumenter,
      ContextPropagators propagators,
      String fullMethodName,
      Metadata headers,
      Context parentContext) {
    this.instrumenter = instrumenter;
    this.propagators = propagators;
    this.fullMethodName = fullMethodName;
    this.headers = headers;
    this.parentContext = parentContext;
    this.startTime = Instant.now();
  }

  @Override
  public io.grpc.Context filterContext(io.grpc.Context context) {
    // 同一请求上可能注册多个本工厂的 tracer，通过 gRPC Context 共享“已处理”标记，防止重复补建 span。
    AtomicBoolean state = STREAM_TRACER_STATE_KEY.get(context);
    if (state == null) {
      return context.withValue(STREAM_TRACER_STATE_KEY, handledOrSpanStarted);
    }
    handledOrSpanStarted = state;
    return context;
  }

  @Override
  public void serverCallStarted(ServerCall<?, ?> call) {
    // 即使其他拦截器提前返回、追踪拦截器没有运行，也不能将已找到的服务方法当成“未注册方法”。
    // gRPC creates a ServerCall only after resolving a registered method. Mark it handled here in
    // case another interceptor short-circuits the call before the tracing interceptor runs.
    handledOrSpanStarted.set(true);
  }

  @Override
  public void streamClosed(Status status) {
    // A ServerCall starts only for a method that is registered on the server, and gRPC closes a
    // stream whose method it could not find with a specific UNIMPLEMENTED status. Requiring both
    // keeps other failures that occur before the ServerCall is created from being reported here.
    if (!isUnknownMethod(status) || handledOrSpanStarted.get()) {
      return;
    }
    // 用 _OTHER 归一化未知方法，原始名称另行保存，避免任意请求路径成为常规方法名。
    GrpcRequest request = new GrpcRequest(UNKNOWN_METHOD_NAME, fullMethodName, headers);
    // startAndEnd 不经过通常的 server start 提取路径，因此先从保留的请求头显式提取远端父上下文。
    Context extracted =
        propagators
            .getTextMapPropagator()
            .extract(parentContext, request, GrpcRequestGetter.INSTANCE);
    // shouldStart 保留抑制规则，CAS 则让共享此状态的 tracer 中至多一个负责补建。
    if (instrumenter.shouldStart(extracted, request)
        && handledOrSpanStarted.compareAndSet(false, true)) {
      InstrumenterUtil.startAndEnd(
          instrumenter, extracted, request, status, status.getCause(), startTime, Instant.now());
    }
  }

  private boolean isUnknownMethod(Status status) {
    // UNIMPLEMENTED 也可能来自业务代码，必须同时匹配 gRPC 的“方法未找到”描述。
    return status.getCode() == Status.Code.UNIMPLEMENTED
        && ("Method not found: " + fullMethodName).equals(status.getDescription());
  }
}
